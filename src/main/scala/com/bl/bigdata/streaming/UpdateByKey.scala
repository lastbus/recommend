package com.bl.bigdata.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.json.JSONObject

/**
 * Created by MK33 on 2016/6/29.
 */
object UpdateByKey {

  def main(args: Array[String]) {

    Logger.getRootLogger.setLevel(Level.WARN)

    if (args.length < 4) {
      System.err.println("Usage: RecommendStreaming <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("spark streaming kafka")
    val sc = new SparkContext(sparkConf)

    val messageCounter = sc.accumulator(0L)
        sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val ssc = new StreamingContext(sc, Seconds(2))
        ssc.checkpoint("/user/spark/checkpoint/kafka/recommend")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaStreaming = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    var received = 0L


    kafkaStreaming.map { s =>
      val json = new JSONObject(s)
      (if (json.getString("actType") == "view") json.getString("goodsId")
      else "IndexGL", 1)
    } .updateStateByKey[Int](updateFunction _)
      .foreachRDD(rdd => rdd.collect().sortWith(_._2 > _._2).take(10).foreach(println))


    ssc.start()
    ssc.awaitTermination()


  }

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    // add the new values with the previous running count to get the new count
    val newCount = newValues.sum + runningCount.getOrElse(0)
    Some(newCount)
  }


}
