package com.bl.bigdata.useranalyze

import com.bl.bigdata.datasource.{Item, ReadData}
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util.{RedisClient, SparkFactory, Tool, ToolRunner}
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD

/**
  * Created by MK33 on 2016/4/11.
  */
class UserCookie extends Tool {

  override def run(args: Array[String]): Unit = {
    Message.addMessage("\n将用户id 和 cookie id 导入 redis：\n")
    val sc = SparkFactory.getSparkContext
    val sql = "select registration_id, cookie_id, event_date from recommendation.memberid_cookieid"
    val rawRDD = ReadData.readHive(sc, sql).map{ case Item(Array(registration, cookie, date)) => (registration, cookie, date) }
    val r = rawRDD.map(r => (r._1, Seq((r._2, r._3)))).reduceByKey(_ ++ _)
      .map(r => ("member_cookie_" + r._1, r._2.sortWith(_._2 > _._2).map(_._1).distinct.mkString("#")))
    val count = sc.accumulator(0)
    saveListToRedis(r, count)
    Message.addMessage(s"\t导入 redis 条数： $count \n")
  }



  def saveListToRedis(rdd: RDD[(String, String)], accumulator: Accumulator[Int]): Unit = {
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


object UserCookie {

  def main(args: Array[String]): Unit = {
    execute(args)
  }

  def execute(args: Array[String]): Unit ={
    val r = new UserCookie with ToolRunner
    r.run(args)
  }

}
