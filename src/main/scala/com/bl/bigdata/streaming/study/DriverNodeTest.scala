package com.bl.bigdata.streaming.study

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK33 on 2016/7/8.
  */
object DriverNodeTest {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val kafkaStream = KafkaUtils.createStream(ssc, "10.201.48.39:2181,10.201.48.40:2181", "count-test", Map("recommend-track" -> 2))

    val a = kafkaStream.count()
    a.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
