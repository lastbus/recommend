package com.bl.bigdata.util

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by MK33 on 2016/4/6.
  */
object HiveDataUtil {
  val fileSystem = FileSystem.get(new Configuration)

  def read(path: String, sc: SparkContext): RDD[String] = {
    val date = new Date
    val before = ConfigurationBL.get("day.before.today", "90").toInt
    val sdf = new SimpleDateFormat("yyyyMMdd")
    var tmp: RDD[String] = sc.parallelize(Seq())

    for (i <- 1 until before;
         d = sdf.format(new Date(date.getTime - 24000L * 3600 * i))
         if exist(path + d)) {
      tmp = tmp ++ sc.textFile(path + d)
    }
    tmp
  }
  def exist(path: String): Boolean = fileSystem.exists(new Path(path))


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(sparkConf)
    val sql = "select cookie_id, member_id, goods_sid from recommendation.user_behavior_raw_data"
    readHive(sql, sc)
    sc.stop()
  }

  def readHive(sql: String, sc: SparkContext): RDD[String] = {
    val hiveContext = new HiveContext(sc)
    val r = hiveContext.sql(sql)
    r.rdd.map(r => r.toString()).map(s=>s.substring(1,s.length - 1))
  }
}
