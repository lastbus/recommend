package com.bl.bigdata.recommend

import com.infomatiq.jsi.Point
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * 地址匹配对应的快到家门店，结果保存在 HBase 中，然后导入生产集群 HBase， Hive 建外部表。
  * Created by MK33 on 2016/10/14.
  */
object AddressStoreToHBase {

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("address store match to HBase"))
    //  加载快到家门店列表
    val stores = sc.textFile("/tmp/store").map(s => {
      val ws = s.split("\t"); (ws(0), ws(1), ws(2), ws(3))
    }).distinct().collect()
    val storesBrd = sc.broadcast(stores)

    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum", "m79sit,s80sit,s81sit")
    hBaseConf.set(TableInputFormat.INPUT_TABLE, "addressCoordinate")
    val hBaseRawData = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2)

//   val conn = ConnectionFactory.createConnection(hBaseConf)
//    val table = conn.getTable(TableName.valueOf("addressCoordinateStore"))
//    table.delete(new Delete(.toBytes("*")))

    hBaseRawData.zipWithIndex().foreachPartition { partition =>
      val hBaseConf2 = HBaseConfiguration.create()
      hBaseConf2.set("hbase.zookeeper.quorum", "m79sit,s80sit,s81sit")
      val conn = ConnectionFactory.createConnection(hBaseConf2)
      val table = conn.getTable(TableName.valueOf("addressCoordinateStore"))
      val shopInfo = new ShopInfo(storesBrd.value)
      val columnFamily = Bytes.toBytes("coordinate")
      val list = new java.util.ArrayList[Put]()

      partition.foreach { case (row, index) =>
        val key = row.getRow
        val latitude = row.getValue(columnFamily, Bytes.toBytes("latitude"))
        val longitude = row.getValue(columnFamily, Bytes.toBytes("longitude"))
        val shopsList = if (longitude != null && latitude != null) {
          val point = new Point(Bytes.toString(longitude).toFloat, Bytes.toString(latitude).toFloat)
          shopInfo.getStores(point)
        } else null
        if (shopsList != null) {
          var i = 0
          for ((storeName, storeLocation, lngLat) <- shopsList) {
            val put = new Put(Bytes.toBytes(Bytes.toString(key) + "-" + i + "-" + index))
            put.addColumn(columnFamily, Bytes.toBytes("address"), key)
            put.addColumn(columnFamily, Bytes.toBytes("longitude"), row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("longitude")))
            put.addColumn(columnFamily, Bytes.toBytes("latitude"), row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("latitude")))
            put.addColumn(columnFamily, Bytes.toBytes("province"), row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("province")))
            put.addColumn(columnFamily, Bytes.toBytes("city"), row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("city")))
            put.addColumn(columnFamily, Bytes.toBytes("citycode"), row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("citycode")))
            put.addColumn(columnFamily, Bytes.toBytes("district"), row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("district")))

            put.addColumn(columnFamily, Bytes.toBytes("storeName"), Bytes.toBytes(storeName))
            put.addColumn(columnFamily, Bytes.toBytes("storeLocation"), Bytes.toBytes(storeLocation))
            //          list.add(put)
            table.put(put)
            i += 1
          }
          //        if (list.size() == 1000) table.put(list)

          /**
            * val longitude = row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("longitude"))
            * val latitude = row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("latitude"))
            * *
            * val province = row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("province"))
            * val city = row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("city"))
            * val citycode = row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("citycode"))
            * val district = row.getValue(Bytes.toBytes("coordinate"), Bytes.toBytes("district"))
            * *
            * (Bytes.toString(key), if (longitude == null) null else Bytes.toDouble(longitude), if (latitude == null) null else Bytes.toDouble(latitude),
            * if (province == null) null else Bytes.toString(province),
            * if (city == null) null else Bytes.toString(city),
            * if (citycode == null) null else Bytes.toString(citycode),
            * if (district == null) null else Bytes.toString(district), shopsList)
            */
        }
      }

      //      table.put(list)
    }
    //.filter(_ != null).map(adds => adds._8.map(s => (adds._1, adds._2, adds._3, adds._4, adds._5, adds._6, adds._7, s._1, s._2, s._3))).flatMap(s => s)
    //.map(s=> s"${s._1} | ${s._2} | ${s._3} | ${s._4} | ${s._5} | ${s._6} | ${s._7} | ${s._8} | ${s._9} | ${s._10} | ${s._10}")


    sc.stop()

  }


}
