package com.bl.bigdata.ranking

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.SparkEnv
import com.bl.bigdata.mail.MailServer
import com.bl.bigdata.util.{ToolRunner, Tool, ConfigurationBL}
import com.redislabs.provider.redis._
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkContext
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
 * 销量排行：
 * 最近一天、七天，n 天，乘上一个权重
 * Created by MK33 on 2016/3/21.
 */
class HotSaleGoods extends Tool with SparkEnv{

  private val logger = LogManager.getLogger(this.getClass)
  private val message = new StringBuilder

  override def run(args: Array[String]): Unit = {
    message.clear()
    message.append("品类热销商品：\n")
    val input = ConfigurationBL.get("user.order.raw.data")
    val output = ConfigurationBL.get("recmd.output")
    val redis = output.contains("redis")
    val local = output.contains("local")

    val deadTimeOne = HotSaleGoods.getDateBeforeNow(ConfigurationBL.get("day.before.today.one").toInt)
    val deadTimeOneIndex = ConfigurationBL.get("hot.sale.index.one").toDouble
    val deadTimeTwo = HotSaleGoods.getDateBeforeNow(ConfigurationBL.get("day.before.today.two").toInt)
    sparkConf.setMaster("local[*]").setAppName("品类热销商品")
    if (redis)
      for ((key, value) <- ConfigurationBL.getAll.filter(_._1.startsWith("redis.")))
        sparkConf.set(key, value)
    val accumulator = sc.accumulator(0)

    val result = sc.textFile(input).map(line => {
      val w = line.split("\t")
      (w(3), w(4), w(6).toDouble, w(7), w(9), w(10))
    })
      .filter(item => HotSaleGoods.filterDate(item, deadTimeTwo))
      .map { case (goodsID, goodsName, sale_num, sale_time, categoryID, category) =>
        (goodsID, (goodsName, if(sale_time >= deadTimeOne) deadTimeOneIndex * sale_num else sale_num, categoryID, category))
      }.reduceByKey((s1, s2) => (s1._1, s1._2 + s2._2, s1._3, s1._4))
      .map { case (goodsID, (goodsName, sale_num, categoryID, category)) =>
        (categoryID, Seq((goodsID, sale_num)))
      }
      .reduceByKey((s1, s2) =>{accumulator += 1; s1 ++ s2})
      .map { case (category, seq) => (category, seq.sortWith(_._2 > _._2).take(20).map(_._1).mkString("#")) }

    if(redis) {
      logger.info("start to write 热销 to redis.")
      sc.toRedisKV(result)
      logger.info("write finished.")
      message.append(s"插入 品类热销商品: $accumulator\n")
    }
    if( local) {
      val jedisPool = new JedisPool(new JedisPoolConfig, "10.201.128.216", 6379) with Serializable
      logger.info("begin to write to local")
      var i = 0
      result.collect().foreach { case (category, ranking) =>
        val jedis = jedisPool.getResource
        jedis.set("rcmd_cate_hotsale_" + category, ranking)
        jedis.close()
        i += 1
      }
      message.append(s"本地插入redis hot sale 商品: $i\n")
      logger.info(s"$i key-values are written to local finished.")
    }
    sc.stop()
  }
}

object HotSaleGoods {

  def main(args: Array[String]): Unit = {
    execute(args)
  }

  def execute(args: Array[String]): Unit ={
    val hotSale = new HotSaleGoods with ToolRunner
    hotSale.run(args)
    MailServer.send(hotSale.message.toString())
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
