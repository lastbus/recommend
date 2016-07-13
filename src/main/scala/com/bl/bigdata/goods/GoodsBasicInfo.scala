package com.bl.bigdata.goods

import com.bl.bigdata.config.{GoodsInfo, HbaseConfig}
import com.bl.bigdata.util.SparkFactory
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/5/31.
 */
class GoodsBasicInfo {

  def goodsInfo(): Unit = {
    val sql = "select sid, mdm_goods_sid, goods_sales_name, goods_type, pro_sid, CASE  WHEN brand_sid IS NULL THEN 'null' else brand_sid END AS brand_sid, CASE  WHEN cn_name IS NULL THEN 'xxx' else cn_name END AS cn_name, category_id, category_name, sale_price, pic_sid, url, channel_sid  " +
      ",stock from recommendation.goods_avaialbe_for_sale_channel where category_id IS NOT NULL"
    val sc = SparkFactory.getSparkContext("goods_info_to_hbase")
    val hiveContext = new HiveContext(sc)
    val goodsRdd = hiveContext.sql(sql).rdd
      .map(row => (row.getString(0), row.getString(1), row.getString(2),
        row.getDouble(3).toString, row.getString(4), row.getString(5),
        row.getString(6), row.getLong(7).toString, row.getString(8),
        row.getDouble(9).toString, row.getString(10), row.getString(11),
        row.getString(12), row.getInt(13)))
    goodsRdd.foreachPartition{
      x=> {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.property.clientPort", HbaseConfig.hbase_zookeeper_property_clientPort)
        conf.set("hbase.zookeeper.quorum",HbaseConfig.hbase_zookeeper_quorum )
        conf.set("hbase.master", HbaseConfig.hbase_master)
        val cookieTbl = new HTable(conf, TableName.valueOf(HbaseConfig.goodstbl))
        cookieTbl.setAutoFlush(false, false)
        cookieTbl.setWriteBufferSize(3*1024*1024)
        x.foreach { y => {
          val p = new Put(Bytes.toBytes(y._1.toString))
          p.addColumn(HbaseConfig.basic_info_family.getBytes, GoodsInfo.mdm_goods_sid.getBytes,
            Bytes.toBytes(y._2.toString))
          p.addColumn(HbaseConfig.basic_info_family.getBytes, GoodsInfo.goods_sales_name.getBytes,
            Bytes.toBytes(y._3.toString))
          p.addColumn(HbaseConfig.basic_info_family.getBytes, GoodsInfo.goods_type.getBytes,
            Bytes.toBytes(y._4.toString))
          p.addColumn(HbaseConfig.basic_info_family.getBytes, GoodsInfo.mdm_goods_sid.getBytes,
            Bytes.toBytes(y._5.toString))
          p.addColumn(HbaseConfig.basic_info_family.getBytes, GoodsInfo.pro_sid.getBytes,
            Bytes.toBytes(y._6.toString))
          p.addColumn(HbaseConfig.basic_info_family.getBytes, GoodsInfo.brand_sid.getBytes,
            Bytes.toBytes(y._7.toString))
          p.addColumn(HbaseConfig.basic_info_family.getBytes, GoodsInfo.cn_name.getBytes,
            Bytes.toBytes(y._8.toString))
          p.addColumn(HbaseConfig.basic_info_family.getBytes, GoodsInfo.category_id.getBytes,
            Bytes.toBytes(y._9.toString))
          p.addColumn(HbaseConfig.basic_info_family.getBytes, GoodsInfo.category_name.getBytes,
            Bytes.toBytes(y._10.toString))
          //sale_price, pic_sid, url, channel_sid
          p.addColumn(HbaseConfig.basic_info_family.getBytes, GoodsInfo.sale_price.getBytes,
            Bytes.toBytes(y._11.toString))
          p.addColumn(HbaseConfig.basic_info_family.getBytes, GoodsInfo.pic_sid.getBytes,
            Bytes.toBytes(y._12.toString))
          p.addColumn(HbaseConfig.basic_info_family.getBytes, GoodsInfo.url.getBytes,
            Bytes.toBytes(y._13.toString))
          p.addColumn(HbaseConfig.basic_info_family.getBytes, GoodsInfo.channel_sid.getBytes,
            Bytes.toBytes(y._14.toString))
          cookieTbl.put(p)
        }
        }
        cookieTbl.flushCommits()
      }
    }
  }

}

object GoodsBasicInfo {
  def main(args: Array[String]) {
    val gb  = new GoodsBasicInfo
    gb.goodsInfo()
  }

}
