package com.bl.bigdata

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by blemall on 4/5/16.
  */
trait SparkEnv {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    private var sparkConf: SparkConf = null
    private var sc: SparkContext = null

    protected def getSparkConf(): SparkConf = {
        if (sparkConf == null){
            sparkConf = new SparkConf()
        }

        sparkConf
    }

    protected def getSparkContext(): SparkContext = {
        if(sc == null){
            sc = new SparkContext()
        }

        sc
    }
}
