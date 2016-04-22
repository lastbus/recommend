package com.bl.bigdata.product

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.{Item, ReadData}
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util.{ConfigurationBL, RedisClient, SparkFactory, Tool}
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
 * 同一款商品不同款式（大小、颜色等等不一样）怎么区分？ 品牌和商品编号一样，销售渠道也一样，但是上线时间可能不一样。
 * Created by MK33 on 2016/4/21.
 */
class GoodsNewArrival extends Tool {

  override def run(args: Array[String]): Unit = {
    Message.addMessage("\nbegin execute calculator goods_new_arrival:\n")
    val prefixPC = ConfigurationBL.get("goods.new.arrival.prefix.pc")
    val prefixAPP = ConfigurationBL.get("goods.new.arrival.prefix.app")

    val sc = SparkFactory.getSparkContext("goods new arrival")

    val sql = "select sid, pro_sid, brand_sid, category_id, channel_sid, sell_start_date " +
      "from recommendation.goods_new_arrival"
    Message.addMessage(s"\thive sql:\n \t\t$sql\n")

    val nowMills = new Date().getTime
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val rawData = ReadData.readHive(sc, sql)
      .map { case Item(Array(sid, productID, bandID, categoryID, channelID, onlineTime)) =>
        (bandID, sid, productID, categoryID, channelID, (nowMills - sdf.parse(onlineTime).getTime) / 1000)
      }.cache()
    val pcRDD = rawData.filter(_._5 == "3").map(s => (s._1, s._2,s._3,s._4,s._6))
    val appRDD = rawData.filter(_._5 == "1").map(s => (s._1, s._2,s._3,s._4,s._6))

    pcRDD.map(s => (s._1, s._3, s._4)).distinct()
    val pcResult = calculate(pcRDD).map(s => (prefixPC + s._1, s._2))
    val pcAccumulator = sc.accumulator(0)
    saveToRedis(pcResult, pcAccumulator)
    Message.addMessage(s"\tpc: insert into redis $prefixPC* :\t $pcAccumulator")

    val appResult = calculate(appRDD).map(s => (prefixAPP + s._1, s._2))
    val appAccumulator = sc.accumulator(0)
    saveToRedis(appResult, appAccumulator)
    Message.addMessage(s"\tpc: insert into redis $prefixAPP* :\t $appAccumulator")

  }

  /** 参数依次为： band_id, sid, product_id, category_id, time before now */
  def calculate(rdd: RDD[(String, String, String, String, Long)]): RDD[(String, String)] ={
    val sc = rdd.sparkContext
    // 每个品牌上线的商品数, 这个也应该不多，查一下有 160 个品牌
    val bandCount = rdd.map(s => ((s._1, s._4))).countByValue().map(s => (s._1._1, s._2.toInt)).toMap
    val bandMapBroadCast = sc.broadcast(bandCount)
    // 统计每个类别上线的商品数, 类别数不多( 267），转换成 map
    val categoryCount = rdd.map(_._4).countByValue().toMap
    val categoryMapBroadCast = sc.broadcast(categoryCount)

    // 根据每个band上线的商品数在所属category的占比，取一定数量的商品，至少取一个。
    val pickGoods = rdd.map{ case (bandID, sid, productID, categoryID, delt) =>
      ((bandID, categoryID), Seq((sid, productID, delt))) }
      .reduceByKey(_ ++ _).map { case (band, prodSeq) => {
      val bandMap = bandMapBroadCast.value
      val categoryMap = categoryMapBroadCast.value
      // 每个品牌取的个数正比与 品牌上线的商品数 / 品牌所属的category上线的商品总数
      val num = Math.ceil(bandMap(band._1).toDouble / categoryMap(band._2).toDouble).toInt
      (band, distinct(prodSeq).sortWith(_._3 < _._3).distinct.take(num))
    }
    }
      .map{case ((band, category), prodSeq) => for (p <- prodSeq) yield (category, Seq((band, p)))}
      .flatMap(s=>s).reduceByKey(_ ++ _)

    val result = pickGoods.map { case (bandID, prodSeq) => (bandID, prodSeq.sortWith(_._2._3 < _._2._3).map(_._2._1).mkString("#")) }
    result
  }

  def distinct(prod: Seq[(String, String, Long)]): Seq[(String, String, Long)] ={
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


  def saveToRedis(rdd: RDD[(String, String)], accumulator: Accumulator[Int]): Unit = {
    val expireTime = ConfigurationBL.get("goods.new.arrival.expire", "604800").toInt
    rdd.foreachPartition(partition => {
      val jedis = RedisClient.pool.getResource
      partition.foreach(s => {
        accumulator += 1
        jedis.set(s._1, s._2)
        jedis.expire(s._1, expireTime) // 过期时间 2 周
      })
      jedis.close()
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
    for (i <- 0 until sortedArray.length) {
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
