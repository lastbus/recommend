package com.bl.bigdata.similarity

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.mail.{Message, MailServer}
import com.bl.bigdata.util._
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Accumulator, SparkContext, SparkConf}
import com.redislabs.provider.redis._

/**
  * 计算用户浏览的物品和购买的物品之间的关联度
  * 计算方法：一天之内用户浏览和购买物品的记录。
  * r(seeGoods, buyGoods) = N(seeGoods, buyGoods) / N(buyGoods)
  * Created by MK33 on 2016/3/15.
  */
class SeeBuyGoodsSimilarity extends Tool {
  private val message = new StringBuilder
  private val logger = LogManager.getLogger(this.getClass.getName)

  override def run(args: Array[String]): Unit = {
    message.clear()
    message.append("看了最终买:\n")
    val inputPath = ConfigurationBL.get("user.behavior.raw.data")
    val output = ConfigurationBL.get("recmd.output")
    val local = output.contains("local")
    val redis = output.contains("redis")

    val sparkConf = new SparkConf().setAppName("看了最终买")
    if (local) sparkConf.setMaster("local[*]")
    if (redis)
      for ((k, v) <- ConfigurationBL.getAll if k.startsWith("redis."))
        sparkConf.set(k, v)

    val sc = SparkFactory.getSparkContext()

    val limit = ConfigurationBL.get("day.before.today", "90").toInt
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = new Date
    val start = sdf.format(new Date(date.getTime - 24000L * 3600 * limit))
    val sql = "select cookie_id, category_sid, event_date, behavior_type, goods_sid from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start"
    val accumulator = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)

    val hiveContext = new HiveContext(sc)
    val rawData = hiveContext.sql(sql).rdd.map(row => ((row.getString(0), row.getString(1),
        {val t = row.getString(2); t.substring(0, t.indexOf(" "))}), row.getString(3), row.getString(4)))
//    val rawData = HiveDataUtil.readHive(sql, sc)
//      // 提取需要的字段
//      .map( line => {
//      //cookie ID, member id, session id, goods id, goods name, quality,
//      // event data, behavior code, channel, category sid, dt
//      val w = line.split("\t")
//      // cookie,商品类别,日期,用户行为编码,商品id
//      ((w(0), w(1), w(6).substring(0, w(6).indexOf(" "))), w(7), w(3))
//    })
    // 用户浏览的商品
    val browserRdd = rawData.filter{ case ((cookie, category, date), behavior, goodsID) => behavior.equals("1000")}
      .map{ case((cookie, category, date), behavior, goodsID) => ((cookie, category, date), goodsID)}.distinct
    // 用户购买的商品
    val buyRdd = rawData.filter{ case ((cookie, category, date), behavior, goodsID) => behavior.equals("4000")}
      .map{ case((cookie, category, date), behavior, goodsID) => ((cookie, category, date), goodsID)}.distinct
    // 用户购买商品数量的统计
    val buyCountRDD = buyRdd.map{ case ((cookie, category, date), goodsID) => (goodsID, 1)}
      .reduceByKey(_ + _)

    // 计算用户浏览和购买的商品之间的相似性：（浏览的商品A， 购买的商品B）的频率除以 B 的频率
    val browserAndBuy = browserRdd.join(buyRdd)
      .map{ case ((cookie, category, date), (goodsIDBrowser, goodsIDBuy)) => ((goodsIDBrowser, goodsIDBuy), 1)}
      .reduceByKey(_ + _)
//      .map{ case ((goodsIDBrowser, goodsIDBuy), count) => (goodsIDBuy, (goodsIDBrowser, count))}
//      .join(buyCountRDD)
//      .map{ case (goodsIDBuy, ((goodsIDBrowser, count), buyCount)) => (goodsIDBrowser, goodsIDBuy, count.toDouble / buyCount)}
      .map{ case ((goodsBrowser, goodsBuy), relation) => (goodsBrowser, Seq((goodsBuy, relation)))}
      .reduceByKey((s1, s2) =>  s1 ++ s2)
      .map(s => { accumulator += 1;("rcmd_shop_" + s._1, s._2.sortWith(_._2 > _._2).take(20).map(_._1).mkString("#")) })
    browserAndBuy.cache()
    message.append(browserAndBuy.count() + "\n")
    message.append(browserAndBuy.first() + "\n")

    // 如果是本地运行，则直接输出，否则保存在 hadoop 中。
    if (redis) {
      saveToRedis2(browserAndBuy, accumulator2)
      message.append(s"rcmd_shop_*: $accumulator")
      message.append(s"插入redis rcmd_shop_*: $accumulator2")

    }
    if (local) browserAndBuy.take(50).foreach(println)
//      sc.toRedisKV(browserAndBuy)
    browserAndBuy.unpersist()

    Message.message.append(message)
    sc.stop()
  }

  def saveToRedis2(rdd: RDD[(String, String)], c: Accumulator[Int]): Unit = {
    rdd.foreachPartition(partition => {
      val jedis = RedisClient.pool.getResource
      partition.foreach(s => {
        jedis.set(s._1, s._2)
        c += 1
      })
      jedis.close()
    })

  }

}

object SeeBuyGoodsSimilarity {

  def main(args: Array[String]) {
    execute(args)
  }

  def execute(args: Array[String]): Unit ={
    val seeBuyGoodsSimilarity = new SeeBuyGoodsSimilarity
    seeBuyGoodsSimilarity.run(args)
//    MailServer.send(seeBuyGoodsSimilarity.message.toString())
  }
}
