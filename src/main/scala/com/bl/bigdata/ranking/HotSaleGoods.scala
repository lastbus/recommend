package com.bl.bigdata.ranking

import java.text.SimpleDateFormat
import java.util.Date

import com.redislabs.provider.redis._
import org.apache.logging.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
 * 销量排行：
 * 最近一天、七天，n 天，乘上一个权重
 * Created by MK33 on 2016/3/21.
 */
object HotSaleGoods {

  private val logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val hotSaleConf = new HotSaleConf()
    hotSaleConf.parseConfFile("hot-sale.xml")
    val output = hotSaleConf.get("hot.sale.output")
    val redis = output.contains("redis")
    val local = output.contains("local")

    val deadTimeOne = getDateBeforeNow(hotSaleConf.get("day.before.today.one").toInt)
    val deadTimeOneIndex = hotSaleConf.get("hot.sale.index.one").toDouble
    val deadTimeTwo = getDateBeforeNow(hotSaleConf.get("day.before.today.two").toInt)

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    if (redis) {
      for ((key, value) <- hotSaleConf.getAll.filter(_._1.startsWith("redis."))){
        sparkConf.set(key, value)
      }
    }
    val sc = new SparkContext(sparkConf)
    val jedisPool = new JedisPool(new JedisPoolConfig, "10.201.128.216", 6379) with Serializable
    val result = sc.textFile("D:\\2016-03-21\\user_order_raw_data").map(line => {
      val w = line.split("\t")
      (w(3), w(4), w(6).toDouble, w(7), w(9), w(10))
    })
      .filter(item => filterDate(item, deadTimeTwo))
      .map { case (goodsID, goodsName, sale_num, sale_time, categoryID, category) =>
        (goodsID, (goodsName, if(sale_time >= deadTimeOne) deadTimeOneIndex * sale_num else sale_num, categoryID, category))
      }.reduceByKey((s1, s2) => (s1._1, s1._2 + s2._2, s1._3, s1._4))
      .map { case (goodsID, (goodsName, sale_num, categoryID, category)) =>
        (categoryID, Seq((goodsID, sale_num)))
      }
      .reduceByKey(_ ++ _)
      .map { case (category, seq) => (category, seq.sortWith(_._2 > _._2).take(20).map(_._1).mkString("#")) }



    if(redis) {
      logger.info("start to write 热销 to redis.")
      sc.toRedisKV(result)
      logger.info("write finished.")
    }
    if( local) {
      logger.info("begin to write to local")
      var i = 0
      result.collect().foreach { case (category, ranking) =>
          val jedis = jedisPool.getResource
          jedis.set("rcmd_cate_hotsale_" + category, ranking)
          jedis.close()
          i += 1
        }
      logger.info(s"$i key-values are written to local finished.")
    }


  }

  /**
   * 计算今天前 n 天的日期
   * @param n 前 n 天
   * @return n 天前的日期
   */
  def getDateBeforeNow(n: Int): String = {
    val now = new Date
    val beforeMill = now.getTime - 24L * 60 * 60 * 1000 * n
    val before = new Date(beforeMill)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    sdf.format(before)
  }


  def filterDate(item: (String, String, Double, String, String, String), deadTime: String): Boolean = {
    item._4 >= deadTime
  }

}
