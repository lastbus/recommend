package com.bl.bigdata.similarity

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Accumulator, SparkConf}

/**
  * 计算用户浏览的某类商品之间的相似度
  * 计算方法：
  * 根据用户在某一天内浏览商品的记录去计算相似度。
  * r(A,B) = N(A,B) / N(B)
  * r(B,A) = N(A,B) / N(A)
  * Created by MK33 on 2016/3/14.
  */
class BrowserGoodsSimilarity extends Tool {

  val isEmpty = (temp: String) => {
    if (temp.trim.length == 0) true
    else if (temp.equalsIgnoreCase("NULL")) true
    else false
  }

  override def run(args: Array[String]): Unit = {
    Message.message.append("看了又看:\n")
    val output = ConfigurationBL.get("recmd.output")
    val local = output.contains("local")
    val redis = output.contains("redis")

    val sparkConf = new SparkConf().setAppName("看了又看")
    if (local) sparkConf.setMaster("local[*]")
    if (redis)
      for ((k, v) <- ConfigurationBL.getAll if k.startsWith("redis."))
        sparkConf.set(k, v)
    val sc = SparkFactory.getSparkContext()
    val accumulator = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)

    val limit = ConfigurationBL.get("day.before.today", "90").toInt
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = new Date
    val start = sdf.format(new Date(date.getTime - 24000L * 3600 * limit))
    val sql = "select cookie_id, category_sid, event_date, behavior_type, goods_s from recommendation.user_behavior_raw_data  where dt >= " + start

    val hiveContext = new HiveContext(sc)
    val rawRdd = hiveContext.sql(sql).rdd
      .map(row => (row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4)))
      // 提取 user 浏览商品的行为
      .filter{ case (cookie, category, date, behaviorId, goodsId) => behaviorId == "1000"}
      .map{ case (cookie, category, date, behaviorId, goodsId) =>
        ((cookie, category, date.substring(0, date.indexOf(" "))), goodsId)
      }.distinct
      .filter(v => {
        val temp = v._1._2
        if (temp.trim.length == 0) false
        else if (temp.equalsIgnoreCase("NULL")) false
        else true
      })
    rawRdd.cache()
    // 将用户看过的商品两两结合在一起
    val tuple = rawRdd.join(rawRdd).filter { case (k, (v1, v2)) => v1 != v2 }
      .map { case (k, (goodId1, goodId2)) => (goodId1, goodId2) }
    // 计算浏览商品 (A,B) 的次数
    val tupleFreq = tuple.map((_, 1)).reduceByKey(_ + _)
    // 计算浏览每种商品的次数
    val good2Freq = rawRdd.map(_._2).map((_, 1)).reduceByKey(_ + _)
    val good1Good2Similarity = tupleFreq.map { case ((good1, good2), freq) => (good2, (good1, freq)) }
      .join(good2Freq)
      .map { case (good2, ((good1, freq), good2Freq)) => (good1, Seq((good2, freq.toDouble / good2Freq))) }
      .reduceByKey((s1, s2) => s1 ++ s2)
      .mapValues(v => { accumulator += 1; v.sortWith(_._2 > _._2).take(20) })
      .map { case (goods1, goods2) => ("rcmd_view_" + goods1, goods2.map(_._1).mkString("#")) }
    // 保存到 redis 中
    rawRdd.unpersist()
    if (redis) {
      saveToRedis(good1Good2Similarity, accumulator2)
      Message.message.append(s"\t\trcmd_view_*: $accumulator\n")
      Message.message.append(s"\t\t插入redis rcmd_view_*: $accumulator2\n")
//      Message.message.append(message)
    }
    if (local) good1Good2Similarity take (50) foreach println
    sc.stop()

  }

  def saveToRedis(rdd: RDD[(String, String)], accumulator: Accumulator[Int]): Unit = {
    rdd.foreachPartition(partition => {
      val jedis = RedisClient.pool.getResource
      partition.foreach(s => {
        accumulator += 1
        jedis.set(s._1, s._2)
      })
      jedis.close()
    })
  }



}

object BrowserGoodsSimilarity {

  def main(args: Array[String]) {
//    val goodsSimilarity = new BrowserGoodsSimilarity with ToolRunner
//    goodsSimilarity.run(args)
//    MailServer.send(goodsSimilarity.message.toString())
        execute(args)
  }

  def execute(args: Array[String]) = {
    val goodsSimilarity = new BrowserGoodsSimilarity with ToolRunner
    goodsSimilarity.run(args)
//    MailServer.send(goodsSimilarity.message.toString())
  }
}
