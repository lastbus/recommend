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
class RealTimeALS {

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
    }.reduceByKey(_ + _).map(x=>(x._1._1,(x._1._2,x._2)))

    val idTransRdd = rawRdd.map { case (uId, (pId, rate)) => uId }.distinct().zipWithUniqueId()
    val jRdd = rawRdd.join(idTransRdd)
    val inputRdd = jRdd.map { case ((mId, ((pId, rate), uId))) => (uId, pId, rate) }
    val users = jRdd.map { case ((mId, ((pId, rate), uId))) => (uId, mId) }.distinct().collect
    val trainRDD = inputRdd.map { case (userIndex, productIndex, rating) =>
      Rating(userIndex.toInt, productIndex.toInt, rating.toDouble)
    }.cache
    val rank = 10
    val iterator = 20
    val model = ALS.train(trainRDD, rank, iterator, 0.01)
    val jedisCluster = RedisClient.jedisCluster
    val upRdd = users.foreach { case (uId, mId) => {
      val K = 10
      val topKRecs = model.recommendProducts(uId.toInt, K)
      val prods = topKRecs.map(r => {
        r.product
      }
      ).mkString("#")
      println(mId+":"+prods)
      jedisCluster.set("rcmd_rt_als_" + mId, prods)
    }
    }
    sc.stop()
  }

}

object RealTimeALS {
  def main(args: Array[String]) {
    val rt = new RealTimeALS
    rt.rtComput
  }}
