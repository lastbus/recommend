package com.bl.bigdata.datasource

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by MK33 on 2016/4/8.
  */
object ReadData {


  def readHive(sc: SparkContext, sql: String): RDD[Item] = {
    val hiveContext = new HiveContext(sc)
    hiveContext.sql(sql).rdd.map(row => Item(row.toSeq.map(_.toString).toArray))
  }

  def readLocal(sc: SparkContext, path: String, delimiter: String = "\t"): RDD[Item] ={
    sc.textFile(path).map(line => Item(line.split(delimiter)))
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("test"))
    val readIn = readLocal(sc, "D:\\2\\dim_category")
    readIn.map{ case Item(Array(one, two, three, four, five, six, seven, eight)) =>
      (one, two, three, four, five, six, seven, eight)}
      .collect().foreach(println)
  }

}
