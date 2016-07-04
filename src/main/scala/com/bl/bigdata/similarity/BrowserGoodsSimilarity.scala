package com.bl.bigdata.similarity

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.commons.cli.{BasicParser, HelpFormatter, Option, Options}
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD

import scala.collection.mutable

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
    BrowserGoodsCommandParser.parse(args)
    logger.info("看了又看开始计算.....")
    Message.addMessage("看了又看:\n")
    val output = BrowserGoodsConf.optionMap(BrowserGoodsConf.output)
    val prefix = BrowserGoodsConf.optionMap(BrowserGoodsConf.prefix)
    val redis = output.contains("redis")
    val sc = SparkFactory.getSparkContext("看了又看")
    val accumulator = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)
    // 根据最近多少天的浏览记录，默认 90天
    val limit = BrowserGoodsConf.optionMap(BrowserGoodsConf.day).toInt
    val behaviorType = BrowserGoodsConf.optionMap(BrowserGoodsConf.behaviorType)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * limit))
    val sql = "select cookie_id, category_sid, event_date, behavior_type, goods_sid  " +
      "from recommendation.user_behavior_raw_data  where dt >= " + start

    val rawRdd = ReadData.readHive(sc, sql).map{ case Array(cookie, category, date, behaviorId, goodsId) =>
                                              (cookie, category, date.substring(0, date.indexOf(" ")), behaviorId, goodsId) }
                                            .filter(_._4 == behaviorType)
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
      val redisType = BrowserGoodsConf.optionMap(BrowserGoodsConf.redisType)
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

object BrowserGoodsCommandParser {
  val options = new Options

  val help = new Option("h", "help", false, "print help information.")
  val input = new Option("i", "input", true, "input datasource type.")
  input.setArgName("datasource")
  val output = new Option("o", "output", true, "output data.")
  output.setArgName("output")
  val day = new Option("n", "number of days", true, "number of days before now.")
  day.setArgName("days")
  val behaviorType = new Option("t", "behavior.type", true, "behavior type")
  behaviorType.setArgName("behaviorCode")
  val redisType = new Option("r", "redis.type", true, "redis type: standalone, sentinel or cluster")
  redisType.setArgName("redisType")

  options.addOption(help)
  options.addOption(input)
  options.addOption(output)
  options.addOption(day)
  options.addOption(behaviorType)
  options.addOption(redisType)

  val commandParser = new BasicParser

  def parse(args: Array[String]): Unit = {

     val commandLine = try {
      commandParser.parse(options, args)
    } catch {
      case e: Exception =>
        println(e.getMessage)
        printHelper()
        sys.exit(-1)
    }

    if (commandLine.hasOption("help")) {
      printHelper()
      sys.exit(-1)
    }

    if (commandLine.hasOption(BrowserGoodsConf.prefix)){
      BrowserGoodsConf.optionMap(BrowserGoodsConf.prefix) = commandLine.getOptionValue(BrowserGoodsConf.prefix)
    }
    if (commandLine.hasOption(BrowserGoodsConf.input)){
      val input = commandLine.getOptionValue(BrowserGoodsConf.input)
      if (input != "sql") {
        System.err.println(s"cannot be $input, only support sql just now.")
        printHelper()
        sys.exit(-1)
      }
      BrowserGoodsConf.optionMap(BrowserGoodsConf.input) = commandLine.getOptionValue(input)
    }
    if (commandLine.hasOption(BrowserGoodsConf.output)){
      val output = commandLine.getOptionValue(BrowserGoodsConf.output)
      if (output != "redis") {
        System.err.println(s"cannot be $output, only support redis just now.")
        printHelper()
        sys.exit(-1)
      }
      BrowserGoodsConf.optionMap(BrowserGoodsConf.output) = commandLine.getOptionValue(BrowserGoodsConf.output)
    }
    if (commandLine.hasOption(BrowserGoodsConf.day)){
      val n = commandLine.getOptionValue(BrowserGoodsConf.day)
      if (!"[1-9]+[0-9]*".r.pattern.matcher(n).matches()) {
        System.err.println("not valid number.")
        sys.exit(-1)
      }
      BrowserGoodsConf.optionMap(BrowserGoodsConf.day) = commandLine.getOptionValue(BrowserGoodsConf.day)
    }
    if (commandLine.hasOption(BrowserGoodsConf.behaviorType)){
      BrowserGoodsConf.optionMap(BrowserGoodsConf.behaviorType) = commandLine.getOptionValue(BrowserGoodsConf.behaviorType)
    }
    if (commandLine.hasOption(BrowserGoodsConf.redisType)){
      BrowserGoodsConf.optionMap(BrowserGoodsConf.redisType) = commandLine.getOptionValue(BrowserGoodsConf.redisType)
    }

  }

  def printHelper(): Unit = {
    val helperFormatter = new HelpFormatter
    helperFormatter.printHelp("BrowserGoodsSimilarity", options)
  }
}

object BrowserGoodsConf {

  val prefix = "prefix"
  val output = "output"
  val day = "day"
  val behaviorType = "behavior.type"
  val input = "sql"
  val redisType = "redis.type"


  val optionMap = mutable.Map(
                  prefix -> "rcmd_view_",
                  input -> "sql",
                  behaviorType -> "1000",
                  output -> "redis",
                  redisType -> "cluster",
                  day -> "90")
}
