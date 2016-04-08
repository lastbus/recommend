package com.bl.bigdata.useranalyze

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

  private val logger = LogManager.getLogger(this.getClass.getName)
  private val message = new StringBuilder

  def run(args: Array[String]): Unit ={
    logger.info("starting to calculator buy goods similarity.")
    message.clear()
    message.append("品类买了还买:\n")
    val inputPath = ConfigurationBL.get("user.behavior.raw.data")
    val inputPath2 = ConfigurationBL.get("dim.category")
    val outPath = ConfigurationBL.get("recmd.output")
    val redis = outPath.contains("redis")
    val local = outPath.contains("local")

    val sparkConf = new SparkConf().setAppName("品类买了还买")
    // 如果在本地测试，将 master 设置为 local 模式
    if (local) sparkConf.setMaster("local[*]")
    if (redis) {
      for ((key, value) <- ConfigurationBL.getAll if key.startsWith("redis."))
        sparkConf.set(key, value)
    }
    val sc = new SparkContext(sparkConf)
    val accumulator = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)

    val limit = ConfigurationBL.get("day.before.today", "90").toInt
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = new Date
    val start = sdf.format(new Date(date.getTime - 24000L * 3600 * limit))
    val sql = "select category_sid, cookie_id, event_date, behavior_type, goods_sid from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start"

//    val buyGoodsRDD = HiveDataUtil.readHive(sql, sc)
//      // 提取的字段: 商品类别,cookie,日期,用户行为编码,商品id
//      .map( line => {
//      val w = line.split("\t")
//      // 商品类别,cookie,日期,用户行为编码,商品id
//      (w(9), (w(0), w(6).substring(0, w(6).indexOf(" ")), w(7), w(3)))
//    })
//      // 提取购买的物品
    val hiveContext = new HiveContext(sc)
    val buyGoodsRDD = hiveContext.sql(sql).rdd
      .map(row => (row.getString(0), (row.getString(1),
        {val t = row.getString(2); t.substring(0, t.indexOf(" "))},
        row.getString(3), row.getString(4))))
      .filter{ case (category, (cookie, date, behaviorID, goodsID)) => behaviorID.equals("4000")}
      .map{case (category, (cookie, date, behaviorID, goodsID)) => (category, (cookie, date, goodsID))}.distinct

    val sql2 = "select category_id, level2_id from recommendation.dim_category"
    val categoriesRDD = hiveContext.sql(sql2).map(row => (row.getLong(0), row.getLong(1))).
      distinct().
      map(s => (s._1.toString, s._2.toString))

//    val categoriesRDD = sc.textFile(inputPath2)
//      // 提取字段
//      .map( line => {
//      val w = line.split("\t")
//      // 商品的末级目录，
//      (w(0), w(w.length - 4))
//    }).distinct

    val buyGoodsKindRDD = buyGoodsRDD.join(categoriesRDD)
      // 将商品的末级类别用一级类别替换
      .map{ case (category, ((cookie, date, goodsID), kind)) => ((cookie, date, kind), category)}
    // 统计每种物品的购买数量
    val buyCount = buyGoodsKindRDD.map{ case ((cookie, date, kind), goodsID) => (goodsID, 1)}.reduceByKey(_ + _)

    // 计算用户购买物品在同一类别下的关联度
    val result0 = buyGoodsKindRDD.join(buyGoodsKindRDD)
      .filter{ case ((cookie, date, kind), (goodsID1, goodsID2)) => goodsID1 != goodsID2}
      .map{ case ((cookie, date, kind), (goodsID1, goodsID2)) => ((goodsID1, goodsID2), 1)}.reduceByKey(_ + _)
      .map{ case ((goodsID1, goodsID2), count) => (goodsID2, (goodsID1, count))}

    val result = result0.join(buyCount)
      .map{ case (goodsID2, ((goodsID1, count), goodsID2Count)) => (goodsID1, goodsID2, count.toDouble / goodsID2Count)}
    val sorting = result.map{ case (goodsID1, goodsID2, similarity) => (goodsID1, Seq((goodsID2, similarity)))}
      .reduceByKey((s1, s2) => s1 ++ s2).mapValues(seq => {accumulator += 1; seq.sortWith(_._2 > _._2).map(_._1).mkString("#")})

    if (redis) {
      logger.info(s"output result to redis, host: ${ConfigurationBL.get("redis.host")}.")
//      sc.toRedisKV(sorting.map(s => ("rcmd_bab_category_" + s._1, s._2)))
      saveToRedis(sorting.map(s => ("rcmd_bab_category_" + s._1, s._2)), accumulator2)
      logger.info("finished to output to redis.")
      message.append(s"rcmd_bab_category_*: $accumulator\n")
      message.append(s"插入redis rcmd_bab_category_*: $accumulator2\n")
    }
    if (local) {
      logger.info("begin to output result to local redis.")
      //TODO 导入本地 redis
      result.take(50).foreach(println)
      logger.info("finished to output result to local redis.")
    }
    Message.message.append(message)
    sc.stop()
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

object CategorySimilarity {

  def main(args: Array[String]): Unit ={
    execute(args)
  }

  def execute(args: Array[String]): Unit ={
    val categorySimilarity = new CategorySimilarity with ToolRunner
    categorySimilarity.run(args)
//    MailServer.send(categorySimilarity.message.toString())
  }
}