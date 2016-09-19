package com.bl.bigdata.machinelearning

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.config.HbaseConfig
import com.bl.bigdata.util.{RedisClient, SparkFactory}
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/7/6.
 */
class HBaseTest {

 // spark-submit --master local  --num-executors 4 --executor-memory 5g --executor-cores 2 --driver-memory 3g --conf 'spark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hbase/lib/*' --driver-class-path  "/opt/cloudera/parcels/CDH/lib/hbase/lib/*"  --class com.bl.bigdata.machinelearning.HBaseTest  tb.jar
  def rtComput(): Unit = {
    val sc = SparkFactory.getSparkContext("rt_als")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", HbaseConfig.hbase_zookeeper_property_clientPort)
    conf.set("hbase.zookeeper.quorum", HbaseConfig.hbase_zookeeper_quorum)
    conf.set("hbase.master", HbaseConfig.hbase_master)
    conf.set(TableInputFormat.INPUT_TABLE, "rcmd_rt_tbl")
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    val rawRdd = hBaseRDD.flatMap { case (key, rs) => {
      val cells = rs.rawCells()
      cells.map(cell => {
        val memberId = new String(CellUtil.cloneRow(cell))
        val goodsId = new String(CellUtil.cloneValue(cell));
        ((memberId, goodsId), 1)
      }
      )
    }
    }
    rawRdd.foreach(println)
    sc.stop()
  }

}

object HBaseTest {
  def main(args: Array[String]) {
    val rt = new HBaseTest
    rt.rtComput
  }}
