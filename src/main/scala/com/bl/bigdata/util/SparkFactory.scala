package com.bl.bigdata.util

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by MK33 on 2016/4/11.
  */
object SparkFactory {
  private[this] var sc: SparkContext = _

  /** */
  def getSparkContext(appName: String, files: String*): SparkContext = {
    if (sc == null) {
      val sparkConf = new SparkConf().setAppName(appName)
      if (files.length > 0) {
        for (file <- files) ConfigurationBL.addResource(file)
        for ((k, v) <- ConfigurationBL.getAll
             if k.startsWith("spark.") || k.startsWith("redis.")) sparkConf.set(k, v)
      }
      sc = new SparkContext(sparkConf)
      sc
    } else {
      // 如果sc已经初始化了，那么参数就没法传递给spark了
      sc
    }
  }

    def getSparkContext(): SparkContext = {
      if (sc == null) getSparkContext("recommend")
      else sc
    }


  def destroyResource(): Unit ={
    if (sc != null) sc.stop()
  }



}
