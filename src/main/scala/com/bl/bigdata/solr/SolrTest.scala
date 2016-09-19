package com.bl.bigdata.solr

import com.bl.bigdata.util.SparkFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.solr.client.solrj.impl.{CloudSolrClient, HttpSolrClient}
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.SparkConf

/**
  * Created by MK33 on 2016/8/26.
  */
object SolrTest {

  val url = "http://s81sit:8983/solr"
  val core = "collection1"
  val docs = Array("abc", "def", "solr")

  def main(args: Array[String]) {
    val a = 11.0
    val b = 10.0
    println(Math.min(Double.PositiveInfinity, a / (a - b)))

//    solrHBase()
//    println(url + "/" + core)
//    val solrClient = new HttpSolrClient(url + "/" + core)
//    val docList = new Array[SolrInputDocument](docs.length)
//    for (i <- 0 until docs.length) {
//      val doc = new SolrInputDocument()
//      doc.addField("id", i)
//      doc.addField("conten_t", docs(i))
//      solrClient.add(doc)
//      solrClient.commit()
//    }

//    solrCloud()
  }


  def solrCloud(): Unit = {

    val cloudSolrClient = new CloudSolrClient("m79sit:2181,s80sit:2181,s81sit:2181/solr")
    val doc1 = new SolrInputDocument()
    doc1.addField("id", 1)
    cloudSolrClient.add(doc1)
    cloudSolrClient.commit()

  }

  def solrHBase(): Unit = {
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum", "m79sit,s80sit,s81sit")
    hBaseConf.set(TableInputFormat.INPUT_TABLE, "member_rate")
    hBaseConf.set("hbase.master", "10.201.129.78:60000")
    val sc = SparkFactory.getSparkContext("test")
    val hbaseRawRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2)

    val tmp = hbaseRawRDD.map { result =>
      val key = Bytes.toString(result.getRow)
      val rate = result.getValue(Bytes.toBytes("rfm"), Bytes.toBytes("rate"))
      (key, rate)
    }.filter(_._2 != null).map( s => (s._1, Bytes.toString(s._2))).foreachPartition { partition =>
      val solrClient = new HttpSolrClient(url + "/" + core)
      partition.foreach { case (memberId, rate) =>
          val doc = new SolrInputDocument()
          doc.addField("id", memberId)
          doc.addField("category", rate)
          doc.addField("content_type", rate)
          solrClient.add(doc)
          solrClient.commit()
      }

    }


  }

}
