package com.bl.bigdata.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK33 on 2016/3/23.
  */
object Test {

  def main(args: Array[String]): Unit ={
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test sql")
    sparkConf.set("log.level", "WARN")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.jsonFile("F:\\spark-1.6.0-bin-hadoop2.6\\examples\\src\\main\\resources\\people.json")
    df.show()
    df.printSchema()



  }
}
