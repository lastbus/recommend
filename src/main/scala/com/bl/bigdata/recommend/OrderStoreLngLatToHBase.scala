package com.bl.bigdata.recommend

import com.infomatiq.jsi.Point
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

/**
  * 将订单数据和线下门店数据做成一个大宽表，保存在HBase中，然后建立 Hive 外部表指向这个大宽表。
  * Created by MK33 on 2016/10/13.
  */
object OrderStoreLngLatToHBase {

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf())
    val hiveContext = new HiveContext(sc)
    //read store information stored in hdfs
    // name, address, longitude/latitude, store zone
    val stores = sc.textFile("hdfs://m78sit:8020/tmp/store").map(s=>{val ws = s.split("\t"); (ws(0), ws(1), ws(2), ws(3))}).distinct().collect()
    // broadcast stores list to every node
    val storesBrd = sc.broadcast(stores)

    val orderSql = " SELECT a.district, a.longitude, a.latitude, o.sale_price, o.sale_sum, o.goods_code, o.goods_name, o.cate_sid,  " +
      "  o.shop_sid, o.shop_name, o.brand_sid, o.brand_name, o.order_no, order2.member_id  " +
      " FROM s03_oms_order_detail  o  " +
      " left join sourcedata.s03_oms_order order2 on o.order_no = order2.order_no  " +
      "  join sourcedata.s03_oms_order_sub o2 on o.order_no = o2.order_no  " +
      "  join addressCoordinate a on a.address = regexp_replace(o2.recept_address_detail, ' ', '')  " +
      "  where o2.recept_address_detail is not NULL AND a.province = '上海市' "

    val orderRawDF = hiveContext.sql(orderSql)

    orderRawDF.map{ row =>
      (row.getString(0),
        if (row.isNullAt(1) || row.get(1).toString.equalsIgnoreCase("null")) -1.0 else row.getDouble(1),
        if (row.isNullAt(2) || row.get(2).toString.equalsIgnoreCase("null")) -1.0 else row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getString(5), row.getString(6), row.getString(7), row.getString(8),
        row.getString(9), row.getString(10), row.getString(11), row.getString(12), row.getString(13))
    }.filter(s => s._2 != -1.0 && s._3 != -1.0).zipWithIndex()
      .foreachPartition{ partition =>
        val storeArray = storesBrd.value
        val shopInfo = new ShopInfo(storeArray)
        val hHbaseConf = HBaseConfiguration.create()
        hHbaseConf.set("hbase.zookeeper.quorum", "m79sit,s80sit,s81sit")
        val conn = ConnectionFactory.createConnection(hHbaseConf)
        val table = conn.getTable(TableName.valueOf("order_lnglat_store_location"))
        val columnFamilyBytes = Bytes.toBytes("info")
      partition.foreach{ case ((distinct , longitude, latitude, salePrice, saleSum,
        goodsCode, goodsName, cateSid, shopId, shopName, brandSid, brandName, orderNo, memberId), index) =>
        val point = new Point(longitude.toFloat, latitude.toFloat)
          val storeNameList = shopInfo.getStores(point)
        var i = 0
          for ((storeName, storeLocation, storeLngLat) <- storeNameList) {
            val put = new Put(Bytes.toBytes(orderNo + "-" + goodsCode + "-" + goodsName + "-" + storeName + "_" + i))
            i+=1
            put.addColumn(columnFamilyBytes, Bytes.toBytes("storeName"), Bytes.toBytes(storeName))
            put.addColumn(columnFamilyBytes, Bytes.toBytes("storeLocation"), Bytes.toBytes(storeLocation))
            put.addColumn(columnFamilyBytes, Bytes.toBytes("storeLngLat"), Bytes.toBytes(storeLngLat))

            if (distinct != null)put.addColumn(columnFamilyBytes, Bytes.toBytes("district"), Bytes.toBytes(distinct))
            if (longitude != null)put.addColumn(columnFamilyBytes, Bytes.toBytes("longitude"), Bytes.toBytes(longitude))
            if (latitude != null)put.addColumn(columnFamilyBytes, Bytes.toBytes("latitude"), Bytes.toBytes(latitude))
            if (salePrice != null)put.addColumn(columnFamilyBytes, Bytes.toBytes("salePrice"), Bytes.toBytes(salePrice))
            if (saleSum != null)put.addColumn(columnFamilyBytes, Bytes.toBytes("saleSum"), Bytes.toBytes(saleSum))
            if (goodsCode != null)put.addColumn(columnFamilyBytes, Bytes.toBytes("goodsCode"), Bytes.toBytes(goodsCode))
            if (goodsName != null)put.addColumn(columnFamilyBytes, Bytes.toBytes("goodsName"), Bytes.toBytes(goodsName))
            if (cateSid != null)put.addColumn(columnFamilyBytes, Bytes.toBytes("cateSid"), Bytes.toBytes(cateSid))
            if (shopId != null)put.addColumn(columnFamilyBytes, Bytes.toBytes("shopId"), Bytes.toBytes(shopId))
            if (shopName != null)put.addColumn(columnFamilyBytes, Bytes.toBytes("shopName"), Bytes.toBytes(shopName))
            if (brandSid != null)put.addColumn(columnFamilyBytes, Bytes.toBytes("brandSid"), Bytes.toBytes(brandSid))
            if (brandName != null)put.addColumn(columnFamilyBytes, Bytes.toBytes("brandName"), Bytes.toBytes(brandName))
            if (orderNo != null)put.addColumn(columnFamilyBytes, Bytes.toBytes("orderNo"), Bytes.toBytes(orderNo))
            if (memberId != null)put.addColumn(columnFamilyBytes, Bytes.toBytes("memberId"), Bytes.toBytes(memberId))

            table.put(put)
          }

      }


      }
  }

}
