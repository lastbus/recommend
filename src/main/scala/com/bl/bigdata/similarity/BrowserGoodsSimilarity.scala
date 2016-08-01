package com.bl.bigdata.similarity

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.commons.cli.Options

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
    if (temp == null)  true
    else if (temp.trim.length == 0) true
    else if (temp.equalsIgnoreCase("NULL")) true
    else false
  }

  override def run(args: Array[String]): Unit = {
    val optionsMap =  try {
      BrowserGoodsCommandParser.parse(args)
    } catch {
      case e: Throwable => logger.error("command line parse error :  " + e)
        BrowserGoodsCommandParser.printHelp
        return
    }

      logger.info("看了又看开始计算.....")
      // 输出参数
      optionsMap.foreach { case (k, v) => logger.info(k + " : " + v)}
      Message.addMessage("看了又看:\n")
      val input = optionsMap(BrowserGoodsCommandParser.input)
      val output = optionsMap(BrowserGoodsCommandParser.output)
      val prefix = optionsMap(BrowserGoodsCommandParser.prefix)
      val redis = output.contains("redis")
      val sc = SparkFactory.getSparkContext("看了又看")
      val accumulator = sc.accumulator(0)
      val accumulator2 = sc.accumulator(0)
      // 根据最近多少天的浏览记录，默认 90天
      val limit = optionsMap(BrowserGoodsCommandParser.days).toInt
      val sdf = new SimpleDateFormat("yyyyMMdd")
      val date0 = new Date
      val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * limit))
      val sql = "select u.cookie_id, u.category_sid, u.event_date, u.behavior_type, u.goods_sid, g.store_sid  " +
        " from recommendation.user_behavior_raw_data u inner join recommendation.goods_avaialbe_for_sale_channel g on g.sid = u.goods_sid  and g.sale_status = 4  " +
        " where u.dt >= " + start
      val sqlName = optionsMap(BrowserGoodsCommandParser.sqlName)
      val raw = DataBaseUtil.getData(input, sqlName, start)
      val rawRdd = raw.filter(a => a(0) != "null" && a(1) != "null" && a(2) != "null" && a(3) != "null" && a(4) != "null")
        .map{ case Array(cookie, category, date, behaviorId, goodsId, storeId) =>
          (cookie, category, date.substring(0, date.indexOf(" ")), behaviorId, goodsId, storeId) }
        .filter(_._4 == "1000")
        .map { case (cookie, category, date, behaviorId, goodsId, storeId) => ((cookie, category, date, storeId), goodsId)}
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
        val redisType = if (output.contains("cluster")) RedisClient.cluster else RedisClient.standalone
        RedisClient.sparkKVToRedis(good1Good2Similarity, accumulator2, redisType)
        Message.addMessage(s"\t\t$prefix*: $accumulator\n")
        Message.addMessage(s"\t\t插入 redis $prefix*: $accumulator2\n")
      }
      rawRdd.unpersist()
      logger.info("看了又看计算结束。")

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

object BrowserGoodsCommandParser {

  val prefix = "goodsIdPrefix"
  val output = "output"
  val sqlName = "sqlName"
  val days = "n-days"
  val input = "input"
  val browserCommand = new MyCommandLine("browser")

  val options = new Options

  browserCommand.addOption("i", input, true, "input datasource type.", "hive")
  browserCommand.addOption("sql", sqlName, true, "hive sql name in configuration file", "browser")
  browserCommand.addOption("o", output, true, "output data.", "redis-cluster")
  browserCommand.addOption("n", days, true, "number of days before now.", "90")
  browserCommand.addOption("p", prefix, true, "number of days before now.", "rcmd_view_")


  def parse(args: Array[String]): Map[String, String] = {
    browserCommand.parser(args)
  }

  def printHelp = browserCommand.printHelper

}
