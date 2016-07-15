package com.bl.bigdata.similarity

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._

/**
  * 1. 统计上午、下午、晚上 购买类目top 20
  * 2. 订单里面分类目单价低于20元top20商品
  * 3. 分析订单里面购买关联类目，哪些类目同时购买
  *
  * 上午： [08:00-12:00)
  * 下午：[12:00-18:00)
  * 晚上：其余
  * Created by MK33 on 2016/3/18.
  */
class BuyActivityStatistic extends Tool {

  override def run(args: Array[String]): Unit = {

    val optionsMap = BuyGoodsSimConf.parse(args)

    logger.info("上午、下午、晚上购买类目开始计算......")
    // 输出参数
    optionsMap.foreach { case (k, v) => logger.info(k + " : " + v)}
    Message.addMessage("上午 下午 晚上 购买类目:\n")

    val input = optionsMap(BuyGoodsSimConf.input)
    val outputPath = optionsMap(BuyGoodsSimConf.output)
    val redis = outputPath.contains("redis")
    val num = optionsMap(BuyActivityConf.top).toInt
    val sc = SparkFactory.getSparkContext("上午 下午 晚上 购买类目")
    val limit = optionsMap(BuyActivityConf.days).toInt
    val sdf = new SimpleDateFormat(optionsMap(BuyActivityConf.sdf))
    val date = new Date
    val start = sdf.format(new Date(date.getTime - 24000L * 3600 * limit))
    val sql = "select category_sid, event_date, behavior_type from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start"

    val sql_1 = optionsMap(BuyActivityConf.sql_1)
    val userRawDataRDD = DataBaseUtil.getData(input, sql_1, start)
    val rawRDD = userRawDataRDD.map{ case Array(category, date1, behavior) =>
                                    (category, date1.substring(date1.indexOf(" ") + 1), behavior)}
                          .filter{ case (category, time, behavior) => behavior.equals("4000") && !category.equalsIgnoreCase("NULL")}
                          .map{ case (category, time, behavior) => (category, time)}

    // 统计上午、下午、晚上 购买类目top 10
    val morning = rawRDD.filter{ case (category, time) => time >= "08" & time < "12:00:00.0"}
                        .map{ s => (s._1, 1)}
                        .reduceByKey(_ + _)
                        .sortBy(_._2, ascending = false)
                        .take(num).map(_._1).mkString("#")

    val noon = rawRDD.filter{ case (category, time) => time >= "12" & time < "18:00:00.0"}
                      .map{ s => (s._1, 1)}
                      .reduceByKey(_ + _)
                      .sortBy(_._2, ascending = false)
                      .take(num).map(_._1).mkString("#")

    val evening = rawRDD.filter{ case (category, time) => time >= "18" | time <= "08"}
                        .map{ s => (s._1, 1)}
                        .reduceByKey(_ + _)
                        .sortBy(_._2, ascending = false)
                        .take(num).map(_._1).mkString("#")

    if (redis){
//      val jedis = new Jedis(ConfigurationBL.get("redis.host"), ConfigurationBL.get("redis.port", "6379").toInt)
      val jedisCluster = RedisClient.jedisCluster
      jedisCluster.set("rcmd_topcategory_forenoon", morning)
      jedisCluster.set("rcmd_topcategory_afternoon", noon)
      jedisCluster.set("rcmd_topcategory_evening", evening)
//      jedis.close()
      Message.addMessage(s"\t上午:\n\t\t$morning\n")
      Message.addMessage(s"\t中午:\n\t\t$noon\n")
      Message.addMessage(s"\t晚上:\n\t\t$evening\n")
    }
    logger.info("上午、下午、晚上购买类目计算结束。")
  }
}

object BuyActivityStatistic {

  def main(args: Array[String]): Unit = {
    execute(args)
  }

  def execute(args: Array[String]): Unit ={
    val buyActivityStatistic = new BuyActivityStatistic with ToolRunner
    buyActivityStatistic.run(args)
  }
}

object BuyActivityConf  {

  val input = "input"
  val sql_1= "sql_1"
  val output = "output"

  val goodsIdPrefixMor = "prefix morning"
  val goodsIdPrefixNoon = "prefix afternoon"
  val goodsIdPrefixEve = "prefix evening"
  val top = "top"
  val days = "n-days"
  val sdf = "dateFormat"


  val buyGoodsSimCommand = new MyCommandLine("buyActivity")
  buyGoodsSimCommand.addOption("i", input, true, "data.source", "hive")
  buyGoodsSimCommand.addOption("o", output, true, "output data to redis， hbase or HDFS", "redis-cluster")
  buyGoodsSimCommand.addOption(sql_1, sql_1, true, "user.behavior.raw.data", "buy.activity.statistic")
  buyGoodsSimCommand.addOption("pm", goodsIdPrefixMor, true, "goods id prefix", "rcmd_topcategory_forenoon")
  buyGoodsSimCommand.addOption("pn", goodsIdPrefixNoon, true, "goods id prefix", "rcmd_topcategory_afternoon")
  buyGoodsSimCommand.addOption("pa", goodsIdPrefixEve, true, "goods id prefix", "rcmd_topcategory_evening")
  buyGoodsSimCommand.addOption("n", days, true, "days before today", "90")
  buyGoodsSimCommand.addOption("sdf", sdf, true, "date format", "yyyyMMdd")
  buyGoodsSimCommand.addOption("top", top, true, "top n goods", "20")

  def parse(args: Array[String]): Map[String, String] ={
    buyGoodsSimCommand.parser(args)
  }

}