package com.bl.bigdata.similarity

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Accumulator, SparkConf}
/**
  * 计算用户购买的物品在一级类目下的关联度。
  * 物体 A 和 B 的关联度：
  * r(A,B) = N(A,B) / N(B)
  * r(B,A) = N(A,B) / N(A)
  * Created by MK33 on 2016/3/16.
  */
class BuyGoodsSimilarity extends Tool{

  override def run(args: Array[String]): Unit = {
    logger.info("starting to calculator buy goods similarity.")
    Message.addMessage("\n买了还买:\n")

    val outPath = ConfigurationBL.get("recmd.output")
    val redis = outPath.contains("redis")
    val local = outPath.contains("local")

    val sparkConf = new SparkConf().setAppName("买了还买")
    // 如果在本地测试，将 master 设置为 local 模式
    if (local) sparkConf.setMaster("local[*]")
    if (redis) {
      for ((key, value) <- ConfigurationBL.getAll if key.startsWith("redis."))
        sparkConf.set(key, value)
    }
    val sc = SparkFactory.getSparkContext
    val accumulator = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)

    val limit = ConfigurationBL.get("day.before.today", "90").toInt
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * limit))
    val sql = "select category_sid, cookie_id, event_date, behavior_type, goods_sid from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start"

    val hiveContext = new HiveContext(sc)
    val buyGoodsRDD = hiveContext.sql(sql).rdd
      .map(row => (row.getString(0), (row.getString(1), {
        val t = row.getString(2); t.substring(0, t.indexOf(" "))
      },
        row.getString(3), row.getString(4))))
      .filter { case (category, (cookie, date, behaviorID, goodsID)) => behaviorID.equals("4000") }
      .map { case (category, (cookie, date, behaviorID, goodsID)) => (category, (cookie, date, goodsID)) }.distinct()

    val sql2 = "select category_id, level2_id from recommendation.dim_category"
    val categoriesRDD = hiveContext.sql(sql2).rdd.map(row => (row.getLong(0), row.getLong(1))).
      distinct().
      map(s => (s._1.toString, s._2.toString))
//    val categoriesRDD = sc.textFile(inputPath2)
//      // 提取字段
//      .map( line => {
//      val w = line.split("\t")
//      // 商品的末级目录，一级目录
//      (w(0), w(w.length - 4))
//    }).distinct

    val buyGoodsKindRDD = buyGoodsRDD.join(categoriesRDD)
      // 将商品的末级类别用一级类别替换
      .map{ case (category, ((cookie, date, goodsID), kind)) => ((cookie, date, kind), goodsID)}
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
      .reduceByKey((s1,s2) => s1 ++ s2).mapValues(seq => {accumulator += 1; seq.sortWith(_._2 > _._2).map(_._1).mkString("#")})

    if (redis) {
      logger.info(s"output result to redis, host: ${ConfigurationBL.get("redis.host")}.")
      saveToRedis(sorting.map(s => ("rcmd_bab_goods_" + s._1, s._2)), accumulator2)
      logger.info("finished to output to redis.")
      Message.addMessage(s"\trcmd_bab_goods_*: $accumulator.\n")
      Message.addMessage(s"\t插入 redis rcmd_bab_goods_*: $accumulator2.\n")
    }
    if (local) {
      logger.info("begin to output result to local redis.")
      //TODO 导入本地 redis
      result.take(50).foreach(println)
      logger.info("finished to output result to local redis.")
    }
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

object BuyGoodsSimilarity {

  def main(args: Array[String]) {
   execute(args)
  }

  def execute(args: Array[String]): Unit ={
    val buyGoodsSimilarity = new BuyGoodsSimilarity with ToolRunner
    buyGoodsSimilarity.run(args)
  }
}
