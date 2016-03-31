package com.bl.bigdata.useranalyze

import com.bl.bigdata.mail.MailServer
import com.bl.bigdata.util.{ConfigurationBL, ToolRunner, Tool}
import org.apache.logging.log4j.LogManager
import org.apache.spark.{SparkContext, SparkConf}
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

    val buyGoodsRDD = sc.textFile(inputPath)
      // 提取的字段: 商品类别,cookie,日期,用户行为编码,商品id
      .map( line => {
      //cookie ID, member id, session id, goods id, goods name, quality,
      // event data, behavior code, channel, category sid, dt
      val w = line.split("\t")
      // 商品类别,cookie,日期,用户行为编码,商品id
      (w(9), (w(0), w(6).substring(0, w(6).indexOf(" ")), w(7), w(3)))
    })
      // 提取购买的物品
      .filter{ case (category, (cookie, date, behaviorID, goodsID)) => behaviorID.equals("4000")}
      .map{case (category, (cookie, date, behaviorID, goodsID)) => (category, (cookie, date, goodsID))}.distinct

    val categoriesRDD = sc.textFile(inputPath2)
      // 提取字段
      .map( line => {
      val w = line.split("\t")
      // 商品的末级目录，
      (w(0), w(w.length - 4))
    }).distinct

    val buyGoodsKindRDD = buyGoodsRDD.join(categoriesRDD)
      // 将商品的末级类别用一级类别替换
      .map{ case (category, ((cookie, date, goodsID), kind)) => ((cookie, date, kind), category)}
    // 统计每种物品的购买数量
    val buyCount = buyGoodsKindRDD.map{ case ((cookie, date, kind), goodsID) => (goodsID, 1)}.reduceByKey(_ + _)

    // 计算用户购买物品在同一类别下的关联度
    val result = buyGoodsKindRDD.join(buyGoodsKindRDD)
      .filter{ case ((cookie, date, kind), (goodsID1, goodsID2)) => goodsID1 != goodsID2}
      .map{ case ((cookie, date, kind), (goodsID1, goodsID2)) => ((goodsID1, goodsID2), 1)}.reduceByKey(_ + _)
      .map{ case ((goodsID1, goodsID2), count) => (goodsID2, (goodsID1, count))}
      .join(buyCount)
      .map{ case (goodsID2, ((goodsID1, count), goodsID2Count)) => (goodsID1, goodsID2, count.toDouble / goodsID2Count)}
    val sorting = result.map{ case (goodsID1, goodsID2, similarity) => (goodsID1, Seq((goodsID2, similarity)))}
      .reduceByKey((s1, s2) => {accumulator += 1; s1 ++ s2}).mapValues(seq => seq.sortWith(_._2 > _._2).map(_._1).mkString("#"))

    if (redis) {
      logger.info(s"output result to redis, host: ${ConfigurationBL.get("redis.host")}.")
      sc.toRedisKV(sorting.map(s => ("rcmd_bab_category_" + s._1, s._2)))
      logger.info("finished to output to redis.")
      message.append(s"插入 rcmd_bab_category_*: $accumulator\n")
    }
    if (local) {
      logger.info("begin to output result to local redis.")
      //TODO 导入本地 redis
      result.take(50).foreach(println)
      logger.info("finished to output result to local redis.")
    }
    sc.stop()
  }
}

object CategorySimilarity {

  def main(args: Array[String]): Unit ={
    execute(args)
  }

  def execute(args: Array[String]): Unit ={
    val categorySimilarity = new CategorySimilarity with ToolRunner
    categorySimilarity.run(args)
    MailServer.send(categorySimilarity.message.toString())
  }
}