package com.bl.bigdata.user

import com.bl.bigdata.config.HbaseConfig
import com.bl.bigdata.util.SparkFactory
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/5/30.
 */
class CookieMapping {

  def cookieMapping(): Unit = {
    val sc = SparkFactory.getSparkContext("cookie mapping")
    val hiveContext = new HiveContext(sc)
    val sql = "select cookie_id,registration_id from recommendation.rt_rcmd_cookie_mapping where  registration_id is not null"
    val cmRdd  = hiveContext.sql(sql).rdd.map(row=>(row.getString(0),row.getString(1)))
    // conf.addResource("hbase-site.xml")
    cmRdd.foreachPartition{
      x=> {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.property.clientPort", HbaseConfig.hbase_zookeeper_property_clientPort)
        //conf.set("hbase.zookeeper.quorum", "10.201.129.81")
        //conf.set("hbase.master", "10.201.129.78:60000")
        conf.set("hbase.zookeeper.quorum",HbaseConfig.hbase_zookeeper_quorum )
        conf.set("hbase.master", HbaseConfig.hbase_master)
        val cookieTbl = new HTable(conf, TableName.valueOf("member_cookie_mapping"))
        cookieTbl.setAutoFlush(false, false)
        cookieTbl.setWriteBufferSize(3*1024*1024)
        x.foreach { y => {
          val p = new Put(Bytes.toBytes(y._1.toString))
          p.addColumn("member_id".getBytes, "member_id".getBytes, Bytes.toBytes(y._2.toString))
          cookieTbl.put(p)
        }
        }
        cookieTbl.flushCommits()
      }
    }
    sc.stop()

  }
}

object CookieMapping {
  def main(args: Array[String]) {
    val cm  = new CookieMapping
    cm.cookieMapping()
    //BuyingCycle.main(null)
  }
}

