package com.bl.bigdata.streaming

import com.bl.bigdata.util.LoggerShutDown
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject


/**
 * Created by MK33 on 2016/5/30.
 */
object RecommendStreaming extends StreamingLogger with LoggerShutDown {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: RecommendStreaming <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("spark streaming kafka")
    val sc = new SparkContext(sparkConf)

    val messageCounter = sc.accumulator(0L)
//    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val ssc = new StreamingContext(sc, Seconds(3))
//    ssc.checkpoint("/user/spark/checkpoint/kafka/recommend")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaStreaming = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    var received = 0L

    kafkaStreaming.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        partition.foreach { record =>
          logger.debug(record)
          try {
            messageCounter += 1
            val json = new JSONObject(record)
            val msgType = json.getString("actType").toLowerCase
            msgType match {
              case "view" => ViewHandler.handle(json)
              case "indexgl" => println("============  index gl   ==============")
              case _ => logger.error("wrong message type: " + msgType)
            }
          } catch {
            case e: Exception => logger.error(s"$record : ${e.getMessage}")
            case e0: Throwable => logger.error(s"super error $record : ${e0.getMessage}")
          }
        }
      }
      if (received < messageCounter.value) {
        logger.info(s"receive ${messageCounter.value} records")
        received = messageCounter.value
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }

}
