package com.bl.bigdata.datasource

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by blemall on 4/5/16.
  *
  */
abstract class HiveDataSource extends DataSource {
    def connect(): HiveContext = {
        val sparkConf = new SparkConf().setAppName("test sql")
        sparkConf.set("log.level", "WARN")
        val sc = new SparkContext(sparkConf)
        val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
        hiveContext
    }





}