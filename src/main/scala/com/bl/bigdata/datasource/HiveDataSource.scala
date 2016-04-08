package com.bl.bigdata.datasource

import com.bl.bigdata.SparkEnv
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by blemall on 4/5/16.
  *
  */
abstract class HiveDataSource extends DataSource with SparkEnv{
    def connect(): HiveContext = {
        val sparkConf = getSparkConf()
        val sc = getSparkContext()
        sparkConf.set("log.level", "WARN")
        val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
        hiveContext
    }
}
