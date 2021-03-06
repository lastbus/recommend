package com.bl.bigdata.similarity

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD

/**
  * 计算用户浏览的物品和购买的物品之间的关联度
  * Created by MK33 on 2016/3/15.
  */
class SeeBuyGoodsSimilarity extends Tool {

  override def run(args: Array[String]): Unit = {
    val optionsMap = SeeBuyCommandLineParser.parse(args)

    logger.info("看了最终买开始计算.....")
    // 输出参数
    optionsMap.foreach { case (k, v) => logger.info(k + " : " + v)}
    Message.message.append("看了最终买:\n")

    val input = optionsMap(SeeBuyCommandLineParser.input)
    val output = optionsMap(SeeBuyCommandLineParser.out)
    val prefix = optionsMap(SeeBuyCommandLineParser.keyPrefix)
    val redis = output.contains("redis")

    val sc = SparkFactory.getSparkContext("看了最终买")
    val limit = optionsMap(SeeBuyCommandLineParser.days).toInt
    val sdf = new SimpleDateFormat(optionsMap(SeeBuyCommandLineParser.sdf))
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * limit))

    val sql = "select u.cookie_id, u.category_sid, u.event_date, u.behavior_type, u.goods_sid, g.store_sid  " +
      "from recommendation.user_behavior_raw_data u inner join recommendation.goods_avaialbe_for_sale_channel g on g.sid = u.goods_sid  and g.sale_status = 4 " +
      s"where u.dt >= $start"
    val sqlName = optionsMap(SeeBuyCommandLineParser.sql)

    val accumulator = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)

    val rawData = DataBaseUtil.getData(input, sqlName, start)
                              .filter(a => a(0) != "null" && a(1) != "null" && a(2) != "null" && a(3) != "null" && a(4) != "null")
                              .map { case Array(cookie, category, date, behaviorId, goodsId, storeId) =>
                                            ((cookie, category, date.substring(0, date.indexOf(" ")), storeId), behaviorId, goodsId)
                              }
    // 用户浏览的商品
    val browserRdd = rawData.filter { case ((cookie, category, date, storeId), behavior, goodsID) => behavior.equals("1000") }
                             .map { case ((cookie, category, date, storeId), behavior, goodsID) => ((cookie, category, date, storeId), goodsID) }.distinct()
    // 用户购买的商品
    val buyRdd = rawData.filter { case ((cookie, category, date, storeId), behavior, goodsID) => behavior.equals("4000") }
                         .map { case ((cookie, category, date, storeId), behavior, goodsID) => ((cookie, category, date,storeId), goodsID) }.distinct()

    // 计算用户浏览和购买的商品之间的相似性：（浏览的商品A， 购买的商品B）的频率除以 B 的频率
    val browserAndBuy = browserRdd.join(buyRdd)
                                  .map{ case ((cookie, category, date, storeId), (goodsIDBrowser, goodsIDBuy)) => ((goodsIDBrowser, goodsIDBuy), 1)}
                                  .filter(item => item._1._1 != item._1._2) // 浏览和购买不能是同一个商品
                                  .reduceByKey(_ + _)
                                  .map{ case ((goodsBrowser, goodsBuy), relation) => (goodsBrowser, Seq((goodsBuy, relation)))}
                                  .reduceByKey((s1, s2) =>  s1 ++ s2)
                                  .map(s => {accumulator += 1; (prefix + s._1, s._2.sortWith(_._2 > _._2).take(20).map(_._1).mkString("#")) })
    browserAndBuy.cache()
    // 如果是本地运行，则直接输出，否则保存在 hadoop 中。
    if (redis) {
      val redisType = if (optionsMap(SeeBuyCommandLineParser.out).contains("cluster")) RedisClient.cluster else RedisClient.standalone
      RedisClient.sparkKVToRedis(browserAndBuy, accumulator2, redisType)
      Message.message.append(s"\t$prefix*: $accumulator\n")
      Message.message.append(s"\t插入 redis $prefix*: $accumulator2\n")
    }
    browserAndBuy.unpersist()
    logger.info("看了最终买计算结束。")
  }

}

object SeeBuyGoodsSimilarity {

  def main(args: Array[String]) {
    execute(args)
  }

  def execute(args: Array[String]): Unit = {
    val seeBuyGoodsSimilarity = new SeeBuyGoodsSimilarity
    seeBuyGoodsSimilarity.run(args)
  }

}

object SeeBuyCommandLineParser {

  val input = "input"
  val sql = "sqlName"
  val out = "output"
  val keyPrefix = "prefix"
  val days = "n-days"
  val sdf = "date.format"

  val seeBuyCommand = new MyCommandLine("SeeBuy")
  seeBuyCommand.addOption("i", input, true, "data source", "hive")
  seeBuyCommand.addOption("o", out, false, "save result to where", "redis-cluster")
  seeBuyCommand.addOption("pre", keyPrefix, true, "output goods prefix", "rcmd_shop_")
  seeBuyCommand.addOption("n", days, true, "n days before today", "90")
  seeBuyCommand.addOption("df", sdf, true, "date format", "yyyyMMdd")
  seeBuyCommand.addOption("sql", sql, true, "sql name in configuration file", "see.buy")

  def parse(args: Array[String]): Map[String, String] = {
    seeBuyCommand.parser(args)
  }

}
