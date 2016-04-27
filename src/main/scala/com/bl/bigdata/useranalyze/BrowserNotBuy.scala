package com.bl.bigdata.useranalyze

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.{Item, ReadData}
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD

/**
  * Created by MK33 on 2016/3/24.
  */
class BrowserNotBuy extends Tool {

  override def run(args: Array[String]): Unit ={
    Message.addMessage("\n最近两个月浏览未购买商品 按时间排序:\n")

    val output = ConfigurationBL.get("recmd.output")
    val prefix = ConfigurationBL.get("browser.not.buy")
    val redis = output.contains("redis")
    val sc = SparkFactory.getSparkContext("最近两个月浏览未购买商品 按时间排序")
    val accumulator = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * 60))
    val sql = "select cookie_id, event_date, behavior_type, category_sid, goods_sid from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start"

    val a = ReadData.readHive(sc, sql).map{ case Item(Array(cookie, date, behavior, category, goods)) =>
        ((cookie, date.substring(0, date.indexOf(" "))), behavior, (category, goods)) }

    val browserRDD = a filter { case ((cookie, date), behaviorID, goodsID) => behaviorID.equals("1000")}
    val buyRDD = a filter { case ((cookie, date), behaviorID, goodsID) => behaviorID.equals("4000")}
    val browserNotBuy = browserRDD subtract buyRDD map (s => (s._1._1, Seq(s._3))) reduceByKey ((s1,s2) => s1 ++ s2) map (item => {
      accumulator += 1
      (prefix + item._1, item._2.distinct.groupBy(_._1).map(s => s._1 + ":" + s._2.map(_._2).mkString(",")).mkString("#"))})

    if (redis) {
      logger.info("starting to output data to redis:")
//      sc toRedisKV browserNotBuy
      saveToRedis(browserNotBuy, accumulator2)
      logger.info("finished to output data to redis.")
      Message.addMessage(s"\t$prefix*: $accumulator\n")
      Message.addMessage(s"\t插入 redis $prefix*: $accumulator2\n")
    }
  }
  /**
    * 将输入的 Array[(category ID, goods ID)] 转换为
    * cateId1:gId1,gId2#cateId2:gid1,gid2#
 *
    * @param items 商品的类别和商品ID数组
    */
  def format(items: Seq[(String, String)]): String = {
    items.groupBy(_._1).map(s => s._1 + ":" + s._2.map(_._2).mkString(",")).mkString("#")
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

object BrowserNotBuy {

  def main(args: Array[String]) {
    execute(args)
  }

  def execute(args:Array[String]): Unit ={
    val browserNotBuy = new BrowserNotBuy with ToolRunner
    browserNotBuy.run(args)
  }
}