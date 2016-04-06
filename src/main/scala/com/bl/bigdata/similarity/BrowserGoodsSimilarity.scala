package com.bl.bigdata.similarity

import com.bl.bigdata.mail.MailServer
import com.bl.bigdata.util.{HiveDataUtil, ConfigurationBL, ToolRunner, Tool}
import org.apache.spark.{SparkConf, SparkContext}
import com.redislabs.provider.redis._

/**
  * 计算用户浏览的某类商品之间的相似度
  * 计算方法：
  * 根据用户在某一天内浏览商品的记录去计算相似度。
  * r(A,B) = N(A,B) / N(B)
  * r(B,A) = N(A,B) / N(A)
  * Created by MK33 on 2016/3/14.
  */
class BrowserGoodsSimilarity extends Tool {

  private val message = new StringBuilder

  val isEmpty = (temp: String) => {
    if (temp.trim.length == 0) true
    else if (temp.equalsIgnoreCase("NULL")) true
    else false
  }

  override def run(args: Array[String]): Unit = {
    message.clear()
    message.append("看了又看:\n")
    val inputPath = ConfigurationBL.get("user.behavior.raw.data")
    val output = ConfigurationBL.get("recmd.output")
    val local = output.contains("local")
    val redis = output.contains("redis")

    val sparkConf = new SparkConf().setAppName("看了又看")
    if (local) sparkConf.setMaster("local[*]")
    if (redis)
      for ((k, v) <- ConfigurationBL.getAll if k.startsWith("redis."))
        sparkConf.set(k, v)
    val sc = new SparkContext(sparkConf)
    val accumulator = sc.accumulator(0)

    val rawRdd = HiveDataUtil.read(inputPath, sc)
      // 提取需要的字段
      .map( line => {
      //cookie ID, member id, session id, goods id, goods name, quality,
      // event data, behavior code, channel, category sid, dt
      val w = line.split("\t")
      // cookie,商品类别,日期,用户行为,商品id
      (w(0), w(9), w(6).substring(0, w(6).indexOf(" ")), w(7), w(3))
    })
      // 提取 user 浏览商品的行为
      .filter{ case (cookie, category, date, behaviorId, goodsId) => behaviorId == "1000"}
      .map{ case (cookie, category, date, behaviorId, goodsId) =>
        ((cookie, category, date), goodsId)
      }.distinct
      .filter(v => {
        val temp = v._1._2
        if (temp.trim.length == 0) false
        else if (temp.equalsIgnoreCase("NULL")) false
        else true
      })
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
      .reduceByKey((s1, s2) => {
        accumulator += 1; s1 ++ s2
      })
      .mapValues(v => v.sortWith(_._2 > _._2).take(20))
      .map { case (goods1, goods2) => ("rcmd_view_" + goods1, goods2.map(_._1).mkString("#")) }
    // 保存到 redis 中
    if (redis) {
      sc.toRedisKV(good1Good2Similarity)
      message.append(s"插入 rcmd_view_*: $accumulator")
    }
    if (local) good1Good2Similarity take (50) foreach println
    sc.stop()

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
    MailServer.send(goodsSimilarity.message.toString())
  }
}