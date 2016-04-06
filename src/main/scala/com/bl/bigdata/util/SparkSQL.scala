package com.bl.bigdata.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by MK33 on 2016/4/5.
  */
object SparkSQL {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("tt")
    val sc = new SparkContext(sparkConf)
    Console println sc.textFile("D:\\2\\dim_category").count()
//    val hiveSql = new HiveContext(sc)
//    hiveSql.sql("CREATE TABLE test_change (a int, b int, c int)")
  }
}
