package com.bl.bigdata.useranalyze

import com.bl.bigdata.util.{ToolRunner, Tool, RedisClient, SparkFactory}
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by MK33 on 2016/4/11.
  */
class UserCookie extends Tool {
  val message = new StringBuilder

  override def run(args: Array[String]): Unit = {
    message.append("将用户id 和 cookie id 导入 redis：")
    val sc = SparkFactory.getSparkContext()
    val hiveContext = new HiveContext(sc)
    val sql = "select registration_id, cookie_id, event_date from recommendation.memberid_cookieid"
    val rawRDD = hiveContext.sql(sql).rdd.map(row => (row.getString(0), row.getString(1), row.getString(2)))

    val r = rawRDD.map(r => (r._1, Seq((r._2, r._3)))).reduceByKey(_ ++ _)
      .map(r => ("member_cookie_" + r._1, r._2.sortWith(_._2 > _._2).map(_._1).mkString("#")))
    val count = sc.accumulator(0)
    saveListToRedis(r, count)
    message.append(s"导入 redis 条数： $count 。")

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

  def main(args: Array[String]) {

  }

  def execute(args: Array[String]): Unit ={
    val r = new UserCookie with ToolRunner
    r.run(args)
  }

}
