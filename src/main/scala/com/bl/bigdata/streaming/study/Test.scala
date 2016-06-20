package com.bl.bigdata.streaming.study

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by MK33 on 2016/6/15.
 */
object Test {

  def main(args: Array[String]) {

    LogManager.getRootLogger.setLevel(Level.WARN)
    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local[2]"))
    val ssc = new StreamingContext(sc, Seconds(1))

    val lines = ssc.socketTextStream("10.201.129.78", 9999)
    val words = lines.flatMap(_.split(" "))
    val pair = words.map((_, 1))
    val wordCounts = pair.reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
