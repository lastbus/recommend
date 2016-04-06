package com.bl.bigdata.datasource

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by blemall on 4/5/16.
  */
object HiveDataSource extends DataSource{
    def read(): DataFrame = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test sql")
        sparkConf.set("log.level", "WARN")
        val sc = new SparkContext(sparkConf)
        //val sqlContext = new SQLContext(sc)
//        val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
//        val f = hiveContext.sql("select * from recommendation.product_properties_raw_data")
//        f
        null
    }
    def main (args: Array[String]) {
        val f = read()
        f.foreach(print)
    }
}
