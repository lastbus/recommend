package com.bl.bigdata.useranalyze

import com.bl.bigdata.util.ConfigurationBL
import org.apache.logging.log4j.LogManager
import org.apache.spark.{SparkContext, SparkConf}
import com.redislabs.provider.redis._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by MK33 on 2016/3/24.
  */
object BrowserNotBuy {
  private val logger = LogManager.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val conf = new ConfigurationBL("browser.xml")
    val input = conf.get("browser.input.path")
    val output = conf.get("browser.output")
    val appName = conf.get("browser.app.name", this.getClass.getName)
    val local = output.contains("local")
    val redis = output.contains("redis")

    val sparkConf = new SparkConf().setAppName(appName)
    if (local) sparkConf.setMaster("local")
    if (redis) {
      for ((key, value) <- conf.getAll if key.contains("redis."))
        sparkConf.set(key, value)
    }
    val sc = new SparkContext(sparkConf)

    val a = sc.textFile(input)
      // 提取的字段: 商品类别,cookie,日期,用户行为编码,商品id
      .map( line => {
      //cookie ID, member id, session id, goods id, goods name, quality,
      // event data, behavior code, channel, category sid, dt
      val w = line.split("\t")
      // cookie,日期,用户行为编码,商品id
      ((w(0), w(6).substring(0, w(6).indexOf(" "))), w(7), (w(9), w(3)))
    })

    val c = Array(("", ""), ("", ""))


    val browserRDD = a filter { case ((cookie, date), behaviorID, goodsID) => behaviorID.equals("1000")}
    val buyRDD = a filter { case ((cookie, date), behaviorID, goodsID) => behaviorID.equals("4000")}
    val browserNotBuy = browserRDD subtract buyRDD map (s => (s._1._1, Seq(s._3))) reduceByKey (_ ++ _) map (item => ("rcmd_cookieid_view_" + item._1, format(item._2.distinct)))

    if (redis) {
      logger info("starting to output data to redis:")
      try {
        sc toRedisKV browserNotBuy
        logger info("finished to output data to redis.")
      } catch {
        case _: Exception => logger error("error to output data to redis.")
      }
    }
    if (local) browserNotBuy take(50) foreach println

    sc.stop()

  }

  /**
    * 将输入的 Array[(category ID, goods ID)] 转换为
    * cateId1:gId1,gId2#cateId2:gid1,gid2#
    * @param items 商品的类别和商品ID数组
    */
  def format(items: Seq[(String, String)]): String = {
    items.groupBy(_._1).map(s => s._1 + ":" + s._2.map(_._2).mkString(",")).mkString("#")
  }
}
