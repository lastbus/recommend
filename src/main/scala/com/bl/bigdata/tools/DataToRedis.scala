package com.bl.bigdata.tools

import com.bl.bigdata.util.RedisUtil
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by blemall on 3/30/16.
  * only for import data to redis server.Many scenes like this in future
  *
  */
object DataToRedis {
    def main (args: Array[String]) {
        val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        val lines = sc.textFile("/home/blemall/workspace/member")
        val rdd = lines.filter(r => r != null).map { r =>
            val parts = r.split("\t")
            (parts(0), parts(1))
        }.distinct().reduceByKey(_ + "#" + _).collect().toMap
        RedisUtil.saveToRedis(sparkConf, rdd)
    }
}
