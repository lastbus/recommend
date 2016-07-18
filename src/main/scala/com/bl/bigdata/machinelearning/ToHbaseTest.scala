package com.bl.bigdata.machinelearning

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.config.HbaseConfig
import com.bl.bigdata.util.SparkFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HTable}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/7/6.
 */
class CookIdMemberIdMapping {

  def toHbase(): Unit = {
    val sc = SparkFactory.getSparkContext("rt_rcmd_cookie_mapping")
    val hiveContext = new HiveContext(sc)
    val sql = "select * from recommendation.rt_rcmd_cookie_mapping"
    //10007103202814643335869 100000000966070 2016-05-27 15:20:32.0
    val ckRdd = hiveContext.sql(sql).rdd.map(row => (row.getString(0), row.getString(1),row.getString(2)))
    ckRdd.foreachPartition{
      x=> {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.property.clientPort", HbaseConfig.hbase_zookeeper_property_clientPort)
        conf.set("hbase.zookeeper.quorum",HbaseConfig.hbase_zookeeper_quorum )
        conf.set("hbase.master", HbaseConfig.hbase_master)
        val cookieTbl = new HTable(conf, TableName.valueOf("member_cookie_mapping"))
        cookieTbl.setAutoFlush(false, false)
        cookieTbl.setWriteBufferSize(3*1024*1024)
        x.foreach { y => {
          val p = new Put(Bytes.toBytes(y._1.toString))
          p.addColumn("member_id".getBytes,"member_id".getBytes,
            Bytes.toBytes(y._2.toString))
          cookieTbl.put(p)
        }
        }
        cookieTbl.flushCommits()
      }
    }
    sc.stop()
  }
}

object CookIdMemberIdMapping {
  def main(args: Array[String]) {
    val rt = new CookIdMemberIdMapping
    rt.toHbase
  }}
