package com.bl.bigdata.similarity

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

/**
  * 计算用户浏览的物品和购买的物品之间的关联度
  * 计算方法：一天之内用户浏览和购买物品的记录。
  * r(seeGoods, buyGoods) = N(seeGoods, buyGoods) / N(buyGoods)
  * Created by MK33 on 2016/3/15.
  */
class SeeBuyGoodsSimilarity extends Tool {

  override def run(args: Array[String]): Unit = {
    Message.message.append("看了最终买:\n")
    val output = ConfigurationBL.get("recmd.output")
    val redis = output.contains("redis")

    val sc = SparkFactory.getSparkContext("看了最终买")

    val limit = ConfigurationBL.get("day.before.today", "90").toInt
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * limit))
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
    val browserRdd = rawData.filter { case ((cookie, category, date), behavior, goodsID) => behavior.equals("1000") }
  .map { case ((cookie, category, date), behavior, goodsID) => ((cookie, category, date), goodsID) }.distinct()
    // 用户购买的商品
    val buyRdd = rawData.filter { case ((cookie, category, date), behavior, goodsID) => behavior.equals("4000") }
      .map { case ((cookie, category, date), behavior, goodsID) => ((cookie, category, date), goodsID) }.distinct()

    // 计算用户浏览和购买的商品之间的相似性：（浏览的商品A， 购买的商品B）的频率除以 B 的频率
    val browserAndBuy = browserRdd.join(buyRdd)
      .map{ case ((cookie, category, date), (goodsIDBrowser, goodsIDBuy)) => ((goodsIDBrowser, goodsIDBuy), 1)}
      .reduceByKey(_ + _)
      .map{ case ((goodsBrowser, goodsBuy), relation) => (goodsBrowser, Seq((goodsBuy, relation)))}
      .reduceByKey((s1, s2) =>  s1 ++ s2)
      .map(s => { accumulator += 1;("rcmd_shop_" + s._1, s._2.sortWith(_._2 > _._2).take(20).map(_._1).mkString("#")) })
    browserAndBuy.cache()

    // 如果是本地运行，则直接输出，否则保存在 hadoop 中。
    if (redis) {
      saveToRedis2(browserAndBuy, accumulator2)
      Message.message.append(s"\trcmd_shop_*: $accumulator\n")
      Message.message.append(s"\t插入redis rcmd_shop_*: $accumulator2\n")

    }
//      sc.toRedisKV(browserAndBuy)
    browserAndBuy.unpersist()
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
  }
}
