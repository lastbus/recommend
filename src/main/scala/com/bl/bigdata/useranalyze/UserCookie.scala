package com.bl.bigdata.useranalyze

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD

/**
  * Created by MK33 on 2016/4/11.
  */
@Deprecated
class UserCookie extends Tool {

  override def run(args: Array[String]): Unit = {
    val optionsMap = UserCookieConf.parser(args)
    val output = optionsMap(UserCookieConf.output)
    logger.info("用户cookie 和用户id 开始计算......")
    // 输出参数
    optionsMap.foreach { case (k, v) => logger.info(k + " : " + v)}
    Message.addMessage("\n用户id 和 cookie \n")
    val sc = SparkFactory.getSparkContext("user cookie")
    val count = sc.accumulator(0)
    val count2 = sc.accumulator(0)
    val sql = "select registration_id, cookie_id, event_date from recommendation.memberid_cookieid"
    val rawRDD = ReadData.readHive(sc, sql).map{ case Array(registration, cookie, date) => (registration, cookie, date) }
    val r = rawRDD.map(r => (r._1, Seq((r._2, r._3)))).reduceByKey(_ ++ _)
                  .map(r => { count += 1
                    ("member_cookie_" + r._1, r._2.sortWith(_._2 > _._2).map(_._1).distinct.mkString("#"))})
    val redisType = if (output.contains(RedisClient.cluster)) RedisClient.cluster else RedisClient.standalone
    RedisClient.sparkKVToRedis(r, count2, redisType)
    Message.addMessage(s"\t member_cookie_*： $count \n")
    Message.addMessage(s"\t插入 redis member_cookie_*： $count2 \n")
    logger.info("用户cookie 和用户id 计算结束。")
  }

}


object UserCookie {

  def main(args: Array[String]): Unit = {
    execute(args)
  }

  def execute(args: Array[String]): Unit ={
    val r = new UserCookie with ToolRunner
    r.run(args)
  }

}

object UserCookieConf  {
  val output = "output"

  val commandLine = new MyCommandLine("userCookie")
  commandLine.addOption("o", output, true, "output result to where", "redis-cluster")

  def parser(args: Array[String]): Map[String, String] ={
    commandLine.parser(args)
  }



}
