package com.bl.bigdata.similarity

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
/**
  * 计算用户购买的物品在一级类目下的关联度。
  * 物体 A 和 B 的关联度：
  * r(A,B) = N(A,B) / N(B)
  * r(B,A) = N(A,B) / N(A)
  * Created by MK33 on 2016/3/16.
  */
class BuyGoodsSimilarity extends Tool{

  override def run(args: Array[String]): Unit = {
    logger.info("买了还买开始计算......")
    Message.addMessage("买了还买:")

    val outPath = ConfigurationBL.get("recmd.output")
    val prefix = ConfigurationBL.get("buy.buy")
    val redis = outPath.contains("redis")
    val sc = SparkFactory.getSparkContext("买了还买")
    val accumulator = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)

    val limit = ConfigurationBL.get("day.before.today", "90").toInt
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * limit))
    val sql = "select category_sid, cookie_id, event_date, behavior_type, goods_sid " +
      " from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start"

    val buyGoodsRDD = ReadData.readHive(sc, sql)
                              .map { case Array(category, cookie, date, behaviorId, goodsId) =>
                                            (category, (cookie, date.substring(0, date.indexOf(" ")), behaviorId, goodsId)) }
                              .filter(_._2._3 == "4000")
                              .map { case (category, (cookie, date, behaviorID, goodsID)) => (category, (cookie, date, goodsID)) }
                              .distinct()

    val sql2 = "select category_id, level2_id from recommendation.dim_category"
    val categoriesRDD = ReadData.readHive(sc, sql2)
                                .map{ case Array(category, level) => (category, level)}
                                .distinct().map(s => (s._1.toString, s._2.toString))
    // 将商品的末级类别用一级类别替换
    val buyGoodsKindRDD = buyGoodsRDD.join(categoriesRDD)
                                      .map { case (category, ((cookie, date, goodsID), kind)) => ((cookie, date, kind), goodsID) }
    // 统计每种物品的购买数量
    val buyCount = buyGoodsKindRDD.map{ case ((cookie, date, kind), goodsID) => (goodsID, 1)}.reduceByKey(_ + _)
    // 计算用户购买物品在同一类别下的关联度
    val result0 = buyGoodsKindRDD.join(buyGoodsKindRDD)
                                  .filter { case ((cookie, date, kind), (goodsID1, goodsID2)) => goodsID1 != goodsID2 }
                                  .map { case ((cookie, date, kind), (goodsID1, goodsID2)) => ((goodsID1, goodsID2), 1) }
                                  .reduceByKey (_ + _)
                                  .map { case ((goodsID1, goodsID2), count) => (goodsID2, (goodsID1, count)) }
    val result = result0.join(buyCount)
                        .map{ case (goodsID2, ((goodsID1, count), goodsID2Count)) => (goodsID1, goodsID2, count.toDouble / goodsID2Count) }
    val sorting = result.map{ case (goodsID1, goodsID2, similarity) => (goodsID1, Seq((goodsID2, similarity)))}
                        .reduceByKey((s1,s2) => s1 ++ s2)
                        .mapValues(seq => { accumulator += 1; seq.sortWith(_._2 > _._2).map(_._1).mkString("#")})
    sorting.persist()
    if (redis) {
      val redisType = ConfigurationBL.get("redis.type")
//      saveToRedis(sorting.map(s => (prefix + s._1, s._2)), accumulator2, redisType)
      RedisClient.sparkKVToRedis(sorting.map(s => (prefix + s._1, s._2)), accumulator2, redisType)
      Message.addMessage(s"\tr$prefix*: $accumulator.\n")
      Message.addMessage(s"\t插入 redis $prefix*: $accumulator2.\n")
    }
    sorting.unpersist()
    logger.info("买了还买计算结束。")

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

object BuyGoodsSimilarity {

  def main(args: Array[String]) {
   execute(args)
  }

  def execute(args: Array[String]): Unit ={
    val buyGoodsSimilarity = new BuyGoodsSimilarity with ToolRunner
    buyGoodsSimilarity.run(args)
  }
}
