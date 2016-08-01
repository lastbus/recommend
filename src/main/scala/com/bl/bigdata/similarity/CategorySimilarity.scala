package com.bl.bigdata.similarity

import java.text.SimpleDateFormat
import java.util.Date


import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD

/**
  * 计算用户购买的物品种类相似度
  * 根据某一天用户购买的物品来计算。
  * 物品AB的相似度计算公式：
  * N(AB)/(N(A) * N(B))^1\2^。
  * N(AB)：同时购买 A 和 B 的订单数
  * N(A)：购买 A 的订单数
  * N(B)：购买 B 的订单数
  * Created by MK33 on 2016/3/18.
  */
class CategorySimilarity extends Tool {

  def run(args: Array[String]): Unit = {
    val optionsMap = try {
      CategorySimConf.parse(args)
    } catch {
      case e: Throwable =>
        logger.error("command line parser error: " + e)
        CategorySimConf.printlnHelp
        return
    }

      logger.info("品类买了还买开始计算......")
      // 输出参数
      optionsMap.foreach { case (k, v) => logger.info(k + " : " + v)}
      Message.addMessage("品类买了还买:\n")

      val input = optionsMap(CategorySimConf.input)
      val output = optionsMap(CategorySimConf.output)
      val prefix = optionsMap(CategorySimConf.goodsIdPrefix)
      val redis = output.contains("redis")

      val sc = SparkFactory.getSparkContext("品类买了还买")
      val accumulator = sc.accumulator(0)
      val accumulator2 = sc.accumulator(0)

      val limit = optionsMap(CategorySimConf.days).toInt
      val sdf = new SimpleDateFormat(optionsMap(CategorySimConf.sdf))
      val date0 = new Date
      val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * limit))
      val sql = "select category_sid, cookie_id, event_date, behavior_type, goods_sid " +
        "from recommendation.user_behavior_raw_data  " +
        s"where dt >= $start"
      val sql1 = optionsMap(CategorySimConf.sql_1)
      val userBehaviorRawDataRDD = DataBaseUtil.getData(input, sql1, start)
      val buyGoodsRDD =userBehaviorRawDataRDD.filter(!_.contains(null))
        .map{ case Array(category, cookie, date, behaviorId, goodsId) =>
          (category, (cookie, date.substring(0, date.indexOf(" ")), behaviorId, goodsId))}
        .filter(_._2._3 == "4000")
        .map { case (category, (cookie, date, behaviorID, goodsID)) => (category, (cookie, date, goodsID)) }
        .distinct()

      val sql_test = "select category_id, level2_id from recommendation.dim_category"
      val sql2 = optionsMap(CategorySimConf.sql_2)
      val categoryRawData = DataBaseUtil.getData(input, sql2)
      val categoriesRDD = categoryRawData.filter(!_.contains("null")).map{ case Array(category, level) => (category, level)}.distinct()
      // 将商品的末级类别用一级类别替换
      val buyGoodsKindRDD = buyGoodsRDD.join(categoriesRDD)
        .map { case (category, ((cookie, date, goodsID), kind)) => ((cookie, date, kind), category) }
      // 统计每种物品的购买数量
      val buyCount = buyGoodsKindRDD.map { case ((cookie, date, kind), goodsID) => (goodsID, 1)}.reduceByKey(_ + _)
      // 计算用户购买物品在同一类别下的关联度
      val result0 = buyGoodsKindRDD.join(buyGoodsKindRDD)
        .filter { case ((cookie, date, kind), (goodsID1, goodsID2)) => goodsID1 != goodsID2}
        .map { case ((cookie, date, kind), (goodsID1, goodsID2)) => ((goodsID1, goodsID2), 1)}
        .reduceByKey(_ + _)
        .map { case ((goodsID1, goodsID2), count) => (goodsID2, (goodsID1, count))}

      val result = result0.join(buyCount)
        .map { case (goodsID2, ((goodsID1, count), goodsID2Count)) => (goodsID1, goodsID2, count.toDouble / goodsID2Count)}
      val sorting = result.map { case (goodsID1, goodsID2, similarity) => (goodsID1, Seq((goodsID2, similarity)))}
        .reduceByKey((s1, s2) => s1 ++ s2)
        .mapValues(seq => { accumulator += 1; seq.sortWith(_._2 > _._2).map(_._1).mkString("#")})
      sorting.persist()
      if (redis) {
        val redisType = if (output.contains(RedisClient.cluster)) RedisClient.cluster else RedisClient.standalone
        logger.info("prepare to save data to redis")
        RedisClient.sparkKVToRedis(sorting.map(s => (prefix + s._1, s._2)), accumulator2, redisType)
        logger.info("save to redis finished")
        Message.message.append(s"\t$prefix*: $accumulator\n")
        Message.message.append(s"\t插入 redis $prefix*: $accumulator2\n")
      }
      sorting.unpersist()
      logger.info("品类买了还买计算结束。")

    }

}

object CategorySimilarity {

  def main(args: Array[String]): Unit ={
    execute(args)
  }

  def execute(args: Array[String]): Unit ={
    val categorySimilarity = new CategorySimilarity with ToolRunner
    categorySimilarity.run(args)
  }
}

object CategorySimConf  {

  val input = "input"
  val sql_1= "sql_1"
  val sql_2 = "sql_2"
  val output = "output"

  val goodsIdPrefix = "prefix"
  val days = "n-days"
  val sdf = "dateFormat"


  val buyGoodsSimCommand = new MyCommandLine("CategorySimilarity")
  buyGoodsSimCommand.addOption("i", input, true, "data.source", "hive")
  buyGoodsSimCommand.addOption("o", output, true, "output data to redis， hbase or HDFS", "redis-cluster")
  buyGoodsSimCommand.addOption(sql_1, sql_1, true, "user.behavior.raw.data", "cate.similarity")
  buyGoodsSimCommand.addOption(sql_2, sql_2, true, "dim.category", "dim.category")
  buyGoodsSimCommand.addOption("p", goodsIdPrefix, true, "goods id prefix", "rcmd_bab_category_")
  buyGoodsSimCommand.addOption("n", days, true, "days before today", "90")
  buyGoodsSimCommand.addOption("sdf", sdf, true, "date format", "yyyyMMdd")

  def parse(args: Array[String]): Map[String, String] = {
    buyGoodsSimCommand.parser(args)
  }

  def printlnHelp = buyGoodsSimCommand.printHelper


}