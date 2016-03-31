package com.bl.bigdata.useranalyze

import com.bl.bigdata.mail.MailServer
import com.bl.bigdata.util.{Tool, ToolRunner, ConfigurationBL}
import org.apache.logging.log4j.LogManager
import org.apache.spark.{SparkContext, SparkConf}
import com.redislabs.provider.redis._

/**
  * Created by MK33 on 2016/3/24.
  */
class BrowserNotBuy extends Tool {

  private val logger = LogManager.getLogger(this.getClass.getName)
  private val message = new StringBuilder

  override def run(args: Array[String]): Unit ={
    message.clear()
    message.append("最近两个月浏览未购买商品 按时间排序:\n")

    val input = ConfigurationBL.get("user.behavior.raw.data")
    val output = ConfigurationBL.get("recmd.output")
    val local = output.contains("local")
    val redis = output.contains("redis")

    val sparkConf = new SparkConf().setAppName("最近两个月浏览未购买商品 按时间排序")
    if (local) sparkConf.setMaster("local")
    if (redis) {
      for ((key, value) <- ConfigurationBL.getAll if key.contains("redis."))
        sparkConf.set(key, value)
    }
    val sc = new SparkContext(sparkConf)
    val accumulator = sc.accumulator(0)

    val a = sc.textFile(input)
      // 提取的字段: 商品类别,cookie,日期,用户行为编码,商品id
      .map( line => {
      //cookie ID, member id, session id, goods id, goods name, quality,
      // event data, behavior code, channel, category sid, dt
      val w = line.split("\t")
      // cookie,日期,用户行为编码,商品id
      ((w(0), w(6).substring(0, w(6).indexOf(" "))), w(7), (w(9), w(3)))
    })

    val browserRDD = a filter { case ((cookie, date), behaviorID, goodsID) => behaviorID.equals("1000")}
    val buyRDD = a filter { case ((cookie, date), behaviorID, goodsID) => behaviorID.equals("4000")}
    val browserNotBuy = browserRDD subtract buyRDD map (s => (s._1._1, Seq(s._3))) reduceByKey ((s1,s2) => {accumulator += 1; s1 ++ s2}) map (item => ("rcmd_cookieid_view_" + item._1, item._2.distinct.groupBy(_._1).map(s => s._1 + ":" + s._2.map(_._2).mkString(",")).mkString("#")))

    if (redis) {
      logger.info("starting to output data to redis:")
      sc toRedisKV browserNotBuy
      logger.info("finished to output data to redis.")
      message.append(s"插入 rcmd_cookieid_view_*: $accumulator\n")
    }
    if (local) browserNotBuy.take(50).foreach(logger.info(_))
    sc.stop()
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
}

object BrowserNotBuy {

  def main(args: Array[String]) {
    execute(args)
  }

  def execute(args:Array[String]): Unit ={
    val browserNotBuy = new BrowserNotBuy with ToolRunner
    browserNotBuy.run(args)
    MailServer.send(browserNotBuy.message.toString())
  }
}