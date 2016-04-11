package com.bl.bigdata.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by MK33 on 2016/4/11.
  */
object HBaseTest {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("hbase test")
    val sc = new SparkContext(sparkConf)
    val tuple = Array(("a", 1), ("b", 2), ("c", 3))
    val testPairRDD = sc.parallelize(tuple).map(s =>{
      val put = new Put(Bytes.toBytes(s._1))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("test"), Bytes.toBytes(s._2))
      (null, put)
    })

    val hbaseConf = sc.hadoopConfiguration
    hbaseConf.addResource("hbase-site.xml")
    println(hbaseConf.get("hbase.zookeeper.property.clientPort"))
    println(hbaseConf.get("hbase.zookeeper.quorum"))
    println(hbaseConf.get("hbase.master"))
//    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
//    hbaseConf.set("hbase.zookeeper.quorum", "10.201.129.81")
//    hbaseConf.set("hbase.master", "10.201.129.78:60000")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "h1")
    val job = new Job(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])
    job.setOutputFormatClass(classOf[TableOutputFormat[Put]])
    testPairRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)

    sc.stop()
  }

}
