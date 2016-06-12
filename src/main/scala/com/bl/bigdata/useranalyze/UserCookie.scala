package com.bl.bigdata.useranalyze

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util.{RedisClient, SparkFactory, Tool, ToolRunner}
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD

/**
  * Created by MK33 on 2016/4/11.
  */
class UserCookie extends Tool {

  override def run(args: Array[String]): Unit = {
    logger.info("用户cookie 和用户id 开始计算......")
    Message.addMessage("\n用户id 和 cookie \n")
    val sc = SparkFactory.getSparkContext("user cookie")
    val count = sc.accumulator(0)
    val count2 = sc.accumulator(0)
    val sql = "select registration_id, cookie_id, event_date from recommendation.memberid_cookieid"
    val rawRDD = ReadData.readHive(sc, sql).map{ case Array(registration, cookie, date) => (registration, cookie, date) }
    val r = rawRDD.map(r => (r._1, Seq((r._2, r._3)))).reduceByKey(_ ++ _)
                  .map(r => { count += 1
                    ("member_cookie_" + r._1, r._2.sortWith(_._2 > _._2).map(_._1).distinct.mkString("#"))})
    saveListToRedis(r, count2)
    Message.addMessage(s"\t member_cookie_*： $count \n")
    Message.addMessage(s"\t插入 redis member_cookie_*： $count2 \n")

    logger.info("用户cookie 和用户id 计算结束。")
  }



  def saveListToRedis(rdd: RDD[(String, String)], accumulator: Accumulator[Int]): Unit = {
    rdd.foreachPartition(partition => {
      try {
        val jedis = RedisClient.pool.getResource
        partition.foreach(s => {
          jedis.set(s._1, s._2)
          accumulator += 1
        })
        jedis.close()
      } catch {
        case e: Exception => Message.addMessage(e.getMessage)
      }
    })
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
