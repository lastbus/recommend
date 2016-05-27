package com.bl.bigdata.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by MK33 on 2016/5/19.
 */
object Test {

  val path = "/tmp/spark-streaming-test"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val ssc = StreamingContext.getOrCreate(path, createStreamingContext)
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newValue = runningCount.getOrElse(0) + newValues.sum
    Option(newValue)
  }

  def createStreamingContext(): StreamingContext = {
    val conf = new SparkConf().setAppName("spark-streaming")
    val ssc = new StreamingContext(conf, Seconds(2))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" ")).map((_, 1))
    val wordCounts = words.reduceByKey(_ + _)
    val w = wordCounts.updateStateByKey[Int](updateFunction _)
    w.print()
    ssc.checkpoint(path)
    ssc
  }


}
