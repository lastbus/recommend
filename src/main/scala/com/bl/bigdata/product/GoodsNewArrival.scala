package com.bl.bigdata.product

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util.{RedisClient, ConfigurationBL, SparkFactory, Tool}
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable._

/**
 * 新上线的商品：
 * 1 导入 redis 中，过期时间 暂定为 2 周，可配置。
 * 2 每个 category 最多选择 N 个，每个品牌推荐的产品数量和该品牌上线的新品数量成正比。
 * 3 最新上线的商品排名越靠前。
 *
 * Created by MK33 on 2016/4/21.
 */
class GoodsNewArrival extends Tool {

  override def run(args: Array[String]): Unit = {
    logger.info("新品上市开始计算........")
    Message.addMessage("\n新商品上市：\n")
    val prefixPC = ConfigurationBL.get("goods.new.arrival.prefix.pc")
    val prefixAPP = ConfigurationBL.get("goods.new.arrival.prefix.app")
    val sc = SparkFactory.getSparkContext("goods new arrival")
    val sql = "select sid, pro_sid, brand_sid, category_id, channel_sid, sell_start_date " +
              "from recommendation.goods_new_arrival"

    val nowMills = new Date().getTime
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val rawData = ReadData.readHive(sc, sql)
                          .map { case Array(sid, productID, bandID, categoryID, channelID, onlineTime) =>
                                    (bandID, sid, productID, categoryID, channelID, (nowMills - sdf.parse(onlineTime).getTime) / 1000)}
                          .cache()
    val pcRDD = rawData.filter(_._5 == "3").map(s => (s._1, s._2,s._3,s._4,s._6))
    val appRDD = rawData.filter(_._5 == "1").map(s => (s._1, s._2,s._3,s._4,s._6))

    val pcResult = calculate(pcRDD).map(s => (prefixPC + s._1, s._2))
    val pcAccumulator = sc.accumulator(0)

    val redisType = ConfigurationBL.get("redis.type")

    saveToRedis(pcResult, pcAccumulator, redisType)
    Message.addMessage(s"\tpc: 插入 redis $prefixPC* :\t $pcAccumulator")

    val appResult = calculate(appRDD).map(s => (prefixAPP + s._1, s._2))
    val appAccumulator = sc.accumulator(0)

    saveToRedis(appResult, appAccumulator, redisType)
//    RedisClient.sparkKVToRedis(appResult, appAccumulator, redisType)
    Message.addMessage(s"\tpc: 插入 redis $prefixAPP* :\t $appAccumulator")

    logger.info("新品上市计算结束。")
  }

  /** 参数依次为： band_id, sid, product_id, category_id, time before now */
  def calculate(rdd: RDD[(String, String, String, String, Long)]): RDD[(String, String)] ={
    val sc = rdd.sparkContext
    // 每个品牌上线的商品数, 这个也应该不多，查一下有 160 个品牌
    val bandCount = rdd.map(s => (s._1, s._4)).countByValue().map(s => (s._1._1, s._2.toInt)).toMap
    val bandMapBroadCast = sc.broadcast(bandCount)
    // 统计每个类别上线的商品数, 类别数不多( 267），转换成 map
    val categoryCount = rdd.map(_._4).countByValue().toMap
    val categoryMapBroadCast = sc.broadcast(categoryCount)

    // 根据每个band上线的商品数在所属category的占比，取一定数量的商品，至少取一个。
    val pickGoods = rdd.map{ case (bandID, sid, productID, categoryID, delt) =>
                                ((bandID, categoryID), mutable.Seq((sid, productID, delt))) }
                        .reduceByKey(_ ++ _)
                        .map { case (band, prodSeq) =>
                                  val bandMap = bandMapBroadCast.value
                                  val categoryMap = categoryMapBroadCast.value
                                  // 每个品牌取的个数正比与 品牌上线的商品数 / 品牌所属的category上线的商品总数
                                  val num = Math.ceil(bandMap(band._1).toDouble / categoryMap(band._2).toDouble).toInt
                                  (band, distinct(prodSeq).sortWith(_._3 < _._3).distinct.take(num))}
                        .map{case ((band, category), prodSeq) => for (p <- prodSeq) yield (category, mutable.Seq((band, p)))}
                        .flatMap(s=>s).reduceByKey(_ ++ _)

    val result = pickGoods.map { case (bandID, prodSeq) => (bandID, prodSeq.sortWith(_._2._3 < _._2._3).map(_._2._1).mkString("#")) }
    result
  }

  def distinct(prod: mutable.Seq[(String, String, Long)]): mutable.Seq[(String, String, Long)] ={
    val b = new ListBuffer[(String, String, Long)]
    val seen = mutable.HashSet[String]()
    for ((band, prodID, time) <- prod) {
      if (!seen(prodID)){
        b += ((band, prodID, time))
        seen += prodID
      }
    }
    b
  }


  def saveToRedis(rdd: RDD[(String, String)], accumulator: Accumulator[Int], redisType: String): Unit = {
    val expireTime = ConfigurationBL.get("goods.new.arrival.expire", "604800").toInt
    rdd.foreachPartition(partition => {
      try {
        redisType match {
          case "cluster" =>
            val jedisCluster = RedisClient.jedisCluster
            partition.foreach { s =>
              jedisCluster.setex(s._1, expireTime.toInt, s._2)
              accumulator += 1
            }
          case "standalone" =>
            val jedis = RedisClient.pool.getResource
            partition.foreach { s =>
              jedis.setex(s._1, expireTime.toInt, s._2)
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

object GoodsNewArrival {

  def main(args: Array[String]) {

  }


  def rmDuplicate[T](sortedArray: Array[(String, T)]): Unit = {
    //TODO 新品推荐，同一个的品牌的商品不能都排在前几位
    var flag = 0
    var sign: String= null
    for (i <- sortedArray.indices) {
      if (sign == null) {
        sign = sortedArray(i)._1
        flag += 1
        if (flag == 5){
          val tmp = sortedArray(i)
          for (j <- i until sortedArray.length){
            sortedArray(j) = sortedArray(j + 1)
          }
          sortedArray(sortedArray.length) = tmp
        }
      }

    }
  }

}
