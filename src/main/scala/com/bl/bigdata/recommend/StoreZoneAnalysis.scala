package com.bl.bigdata.recommend

import com.infomatiq.jsi.Point
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * 读取订单数据，查询收货地址然后保存在 hdfs 上面，这里有一个问题，跑的时间太长了
  * Created by MK33 on 2016/10/12.
  */
@Deprecated
object StoreZoneAnalysis {

  def main(args: Array[String]) {
//    Logger.getRootLogger.setLevel(Level.WARN)

    val sc = new SparkContext(new SparkConf().setAppName("order analysis"))
    // name, address, longitude/latitude, store zone
    val stores = sc.textFile("hdfs://m78sit:8020/tmp/store").map(s=>{val ws = s.split("\t"); (ws(0), ws(1), ws(2), ws(3))}).distinct().collect()
    val storesBrd = sc.broadcast(stores)

    val orderSql = " SELECT a.district, a.longitude, a.latitude, o.sale_price, o.sale_sum, o.goods_code, o.goods_name, o.cate_sid,  " +
      "  o.shop_sid, o.shop_name, o.brand_sid, o.brand_name, o.order_no, order2.member_id  " +
      " FROM s03_oms_order_detail  o  " +
      " left join sourcedata.s03_oms_order order2 on o.order_no = order2.order_no  " +
      "  join sourcedata.s03_oms_order_sub o2 on o.order_no = o2.order_no  " +
      "  join addressCoordinate a on a.address = regexp_replace(o2.recept_address_detail, ' ', '')  " +
      "  where o2.recept_address_detail is not NULL AND a.province = '上海市' "

//    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val spark = new HiveContext(sc)
    val orderRawDF = spark.sql(orderSql)
//    orderRawDF.createOrReplaceTempView("order")

    val path1 = "/tmp/order_store2"
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val path = new Path(path1)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }

    orderRawDF.map{ row =>
      (row.getString(0),
        if (row.isNullAt(1) || row.get(1).toString.equalsIgnoreCase("null")) -1.0 else row.getDouble(1),
        if (row.isNullAt(2) || row.get(2).toString.equalsIgnoreCase("null")) -1.0 else row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getString(5), row.getString(6), row.getString(7), row.getString(8),
        row.getString(9), row.getString(10), row.getString(11), row.getString(12), row.getString(13))
    }.filter(s => s._2 != -1.0 && s._3 != -1.0).map{ case (distinct , longitude, latitude, salePrice, saleSum,
      goodsCode, goodsName, cateSid, shopId, shopName, brandSid, brandName, orderNo, memberId) =>

        val point = new Point(longitude.toString.toFloat, latitude.toString.toFloat)
        val storeName = new ShopInfo(storesBrd.value).getStores(point)
        val a = for ((name, street, lngLat) <- storeName) yield
          (name, street, lngLat, distinct, longitude, latitude, salePrice, saleSum, goodsCode, goodsName, cateSid, shopId, shopName, brandSid, brandName, orderNo, memberId)
        a
    }.flatMap(s => s).saveAsTextFile(path1)


//    val path2 = "/tmp/order_store_lng_lat"
//    val path22 = new Path(path2)
//    if (fs.exists(path22)) {
//      fs.delete(path22, true)
//    }
//
//    orderRawDF.map{ row =>
//      (row.getString(0), row.getDouble(1), row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getString(5), row.getString(6), row.getString(7), row.getString(8),
//        row.getString(9), row.getString(10), row.getString(11), row.getString(12), row.getString(13))
//    }.map{ case (distinct, longitude, latitude, salePrice, saleSum,
//    goodsCode, goodsName, cateSid, shopId, shopName, brandSid, brandName, orderNo, memberId) =>
//      s"$distinct\t$longitude\t$latitude\t$salePrice\t$saleSum\t$goodsCode\t$goodsName\t$cateSid\t$shopId\t$shopName\t$brandSid\t$brandName\t$orderNo\t$memberId"
//        (distinct, longitude, latitude, salePrice, saleSum, goodsCode, goodsName, cateSid, shopId, shopName, brandSid, brandName, orderNo, memberId)
//    }.coalesce(10).saveAsTextFile("/tmp/order_store_lng_lat")

    sc.stop()


  }

}
