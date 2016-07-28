package com.bl.bigdata.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by MK33 on 2016/6/29.
 */
object TT {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("test"))
    val ssc = new StreamingContext(sc, Seconds(2))

    ssc.sparkContext




  }
}
