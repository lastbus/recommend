package com.bl.bigdata.similarity

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD

/**
  * Created by MK33 on 2016/3/24.
  */
class BrowserNotBuy extends Tool {

  override def run(args: Array[String]): Unit = {
    val optionsMap = BrowserNotBuyConf.parse(args)
    logger.info("最近两个月浏览未购买商品开始计算......")
    // 输出参数
    optionsMap.foreach { case (k, v) => logger.info(k + " : " + v)}
    Message.addMessage("\n最近两个月浏览未购买商品 按时间排序:\n")
    val input = optionsMap(BrowserNotBuyConf.input)
    val output = optionsMap(BrowserNotBuyConf.output)
    val prefix = optionsMap(BrowserNotBuyConf.goodsIdPrefix)
    val redis = output.contains("redis")
    val sc = SparkFactory.getSparkContext("最近两个月浏览未购买商品 按时间排序")
    val accumulator = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)
    val sdf = new SimpleDateFormat(optionsMap(BrowserNotBuyConf.sdf))
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * 60))
    val sql = "select cookie_id, event_date, behavior_type, category_sid, goods_sid " +
              " from recommendation.user_behavior_raw_data  " +
              s"where dt >= $start"

    val sqlName = optionsMap(BrowserNotBuyConf.sql)
    val rawRDD = DataBaseUtil.getData(input, sqlName, start)
    val a = rawRDD.filter(!_.contains("null")).map { case Array(cookie, date, behavior, category, goods) =>
                                                  ((cookie, date.substring(0, date.indexOf(" "))), behavior, (category, goods)) }

    val browserRDD = a filter { case ((cookie, date), behaviorID, goodsID) => behaviorID.equals("1000") }
    val buyRDD = a filter { case ((cookie, date), behaviorID, goodsID) => behaviorID.equals("4000") }
    val browserNotBuy = browserRDD subtract buyRDD map (s => (s._1._1, Seq((s._3._1, (s._3._2, s._1._2))))) reduceByKey ((s1,s2) => s1 ++ s2) map (item => {
                                    accumulator += 1
                                    (prefix + item._1, item._2.groupBy(_._1).map(s => s._1 + ":" + s._2.map(_._2).sortWith(_._2 > _._2).map(_._1).distinct.mkString(",")).mkString("#"))})
    browserNotBuy.persist()
    if (redis) {
      val redisType = if (output.contains("cluster")) RedisClient.cluster else RedisClient.standalone
      RedisClient.sparkKVToRedis(browserNotBuy, accumulator2, redisType)
      Message.addMessage(s"\t$prefix*: $accumulator\n")
      Message.addMessage(s"\t插入 redis $prefix*: $accumulator2\n")
    }
    browserNotBuy.unpersist()
    logger.info("最近两个月浏览未购买商品计算结束。")
  }

  /**
    * 将输入的 Array[(category ID, goods ID)] 转换为
    * cateId1:gId1,gId2#cateId2:gid1,gid2#
    * @param items 商品的类别和商品ID数组
    */
  def format(items: Seq[(String, String)]): String = {
    items.groupBy(_._1).map(s => s._1 + ":" + s._2.map(_._2).mkString(",")).mkString("#")
  }

}

object BrowserNotBuy {

  def main(args: Array[String]) {
    execute(args)
  }

  def execute(args:Array[String]): Unit ={
    val browserNotBuy = new BrowserNotBuy with ToolRunner
    browserNotBuy.run(args)
  }
}

object BrowserNotBuyConf  {

  val input = "input"
  val sql= "sql"
  val output = "output"

  val goodsIdPrefix = "goods id prefix"
  val top = "top"
  val days = "n-days"
  val sdf = "dateFormat"


  val buyGoodsSimCommand = new MyCommandLine("browserNotBuy")
  buyGoodsSimCommand.addOption("i", input, true, "data.source", "hive")
  buyGoodsSimCommand.addOption("o", output, true, "output data to redis， hbase or HDFS", "redis-cluster")
  buyGoodsSimCommand.addOption(sql, sql, true, "user.behavior.raw.data", "browser.not.buy")
  buyGoodsSimCommand.addOption("pm", goodsIdPrefix, true, "goods id prefix", "rcmd_cookieid_view_")
  buyGoodsSimCommand.addOption("n", days, true, "days before today", "90")
  buyGoodsSimCommand.addOption("sdf", sdf, true, "date format", "yyyyMMdd")
  buyGoodsSimCommand.addOption("top", top, true, "top n goods", "20")

  def parse(args: Array[String]): Map[String, String] ={
    buyGoodsSimCommand.parser(args)
  }

}