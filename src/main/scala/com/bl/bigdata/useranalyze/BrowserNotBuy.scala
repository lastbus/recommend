package com.bl.bigdata.useranalyze

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Accumulator, SparkConf}

/**
  * Created by MK33 on 2016/3/24.
  */
class BrowserNotBuy extends Tool {

  private val logger = LogManager.getLogger(this.getClass.getName)

  override def run(args: Array[String]): Unit ={
    Message.addMessage("\n最近两个月浏览未购买商品 按时间排序:\n")

    val output = ConfigurationBL.get("recmd.output")
    val local = output.contains("local")
    val redis = output.contains("redis")

    val sparkConf = new SparkConf().setAppName("最近两个月浏览未购买商品 按时间排序")
    if (local) sparkConf.setMaster("local")
    if (redis) {
      for ((key, value) <- ConfigurationBL.getAll if key.contains("redis."))
        sparkConf.set(key, value)
    }
    val sc = SparkFactory.getSparkContext()
    val accumulator = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)


    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * 60))
    val sql = "select cookie_id, event_date, behavior_type, category_sid, goods_sid from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start"

    val hive = new HiveContext(sc)
    val a = hive.sql(sql).rdd.map(row => ((row.getString(0), {val d = row.getString(1); d.substring(0, d.indexOf(" "))}),
      row.getString(2), (row.getString(3), row.getString(4))))
//    val a = HiveDataUtil.readHive(sql, sc)
//      // 提取的字段: 商品类别,cookie,日期,用户行为编码,商品id
//      .map( line => {
//      //cookie ID, member id, session id, goods id, goods name, quality,
//      // event data, behavior code, channel, category sid, dt
//      val w = line.split("\t")
//      // cookie,日期,用户行为编码,商品id
//      ((w(0), w(6).substring(0, w(6).indexOf(" "))), w(7), (w(9), w(3)))
//    })

    val browserRDD = a filter { case ((cookie, date), behaviorID, goodsID) => behaviorID.equals("1000")}
    val buyRDD = a filter { case ((cookie, date), behaviorID, goodsID) => behaviorID.equals("4000")}
    val browserNotBuy = browserRDD subtract buyRDD map (s => (s._1._1, Seq(s._3))) reduceByKey ((s1,s2) => s1 ++ s2) map (item => {
      accumulator += 1
      ("rcmd_cookieid_view_" + item._1, item._2.distinct.groupBy(_._1).map(s => s._1 + ":" + s._2.map(_._2).mkString(",")).mkString("#"))})

    if (redis) {
      logger.info("starting to output data to redis:")
//      sc toRedisKV browserNotBuy
      saveToRedis(browserNotBuy, accumulator2)
      logger.info("finished to output data to redis.")
      Message.addMessage(s"\trcmd_cookieid_view_*: $accumulator\n")
      Message.addMessage(s"\t插入redis rcmd_cookieid_view_*: $accumulator2\n")
    }
    if (local) browserNotBuy.take(50).foreach(logger.info(_))
  }
  /**
    * 将输入的 Array[(category ID, goods ID)] 转换为
    * cateId1:gId1,gId2#cateId2:gid1,gid2#
 *
    * @param items 商品的类别和商品ID数组
    */
  def format(items: Seq[(String, String)]): String = {
    items.groupBy(_._1).map(s => s._1 + ":" + s._2.map(_._2).mkString(",")).mkString("#")
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

object BrowserNotBuy {

  def main(args: Array[String]) {
    execute(args)
  }

  def execute(args:Array[String]): Unit ={
    val browserNotBuy = new BrowserNotBuy with ToolRunner
    browserNotBuy.run(args)
  }
}