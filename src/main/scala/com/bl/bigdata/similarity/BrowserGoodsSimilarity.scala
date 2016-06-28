package com.bl.bigdata.similarity

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD

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
    logger.info("看了又看开始计算.....")
    Message.addMessage("看了又看:\n")
    val output = ConfigurationBL.get("recmd.output")
    val prefix = ConfigurationBL.get("browser.again")
    val redis = output.contains("redis")
    val sc = SparkFactory.getSparkContext("看了又看")
    val accumulator = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)
    // 根据最近多少天的浏览记录，默认 90天
    val limit = ConfigurationBL.get("day.before.today", "90").toInt
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * limit))
    val sql = "select cookie_id, category_sid, event_date, behavior_type, goods_sid  " +
      "from recommendation.user_behavior_raw_data  where dt >= " + start

    val rawRdd = ReadData.readHive(sc, sql).map{ case Array(cookie, category, date, behaviorId, goodsId) =>
                                              (cookie, category, date.substring(0, date.indexOf(" ")), behaviorId, goodsId) }
                                            .filter(_._4 == "1000")
                                            .map { case (cookie, category, date, behaviorId, goodsId) => ((cookie, category, date), goodsId)}
                                            .filter(v => {
                                              if (v._1._2.trim.length == 0) false
                                              else if (v._1._2.equalsIgnoreCase("NULL")) false
                                              else true })
                                            .distinct()
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
                                        .map { case (good2, ((good1, freq), good2Freq1)) => (good1, Seq((good2, freq.toDouble / good2Freq1))) }
                                        .reduceByKey((s1, s2) => s1 ++ s2)
                                        .mapValues(v => v.sortWith(_._2 > _._2).take(20))
                                        .map { case (goods1, goods2) => accumulator += 1; (prefix + goods1, goods2.map(_._1).mkString("#")) }

    // 保存到 redis 中
    if (redis) {
      val redisType = ConfigurationBL.get("redis.type")
//      saveToRedis(good1Good2Similarity, accumulator2, redisType)
      RedisClient.sparkKVToRedis(good1Good2Similarity, accumulator2, redisType)
      Message.addMessage(s"\t\t$prefix*: $accumulator\n")
      Message.addMessage(s"\t\t插入 redis $prefix*: $accumulator2\n")
    }
    rawRdd.unpersist()
    logger.info("看了又看计算结束。")
  }

  def saveToRedis(rdd: RDD[(String, String)], accumulator: Accumulator[Int], redisType: String): Unit = {
    rdd.foreachPartition(partition => {
      try {
        redisType match {
          case "cluster" =>
            val jedis = RedisClient.jedisCluster
            partition.foreach { s =>
              jedis.set(s._1, s._2)
              accumulator += 1
            }
            jedis.close()
          case "standalone" =>
            val jedis = RedisClient.pool.getResource
            partition.foreach { s =>
              jedis.set(s._1, s._2)
              accumulator += 1
            }
            jedis.close()
          case _ => logger.error(s"wrong redis type $redisType ")
        }

      } catch {
        case e: Exception => Message.addMessage(e.getMessage)
      }
    })
  }

}

object BrowserGoodsSimilarity {

  def main(args: Array[String]) {
        execute(args)
  }

  def execute(args: Array[String]) = {
    val goodsSimilarity = new BrowserGoodsSimilarity with ToolRunner
    goodsSimilarity.run(args)
  }
}
