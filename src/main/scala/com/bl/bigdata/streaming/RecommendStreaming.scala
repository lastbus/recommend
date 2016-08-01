package com.bl.bigdata.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.util.{LoggerShutDown, MyCommandLine}
import kafka.serializer.StringDecoder
import org.apache.commons.cli.{BasicParser, HelpFormatter, Option, Options}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

import scala.collection.mutable


/**
 * Created by MK33 on 2016/5/30.
 */
object RecommendStreaming extends StreamingLogger  {

  def main(args: Array[String]) {
    val optionMap = RecommendStreamingCommandParser.parse(args)

    println("program parameters:")
    for (key <- optionMap.keys){
      println(s"\t\t\t${key} = ${optionMap(key)}")
    }
    println(s"=========  begin to execute :  ${new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date())} =========")

    val writeAhead = optionMap(RecommendStreamingCommandParser.writeAheadLog).toBoolean
    val debug = optionMap(RecommendStreamingCommandParser.debug).toBoolean
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val messageCounter = sc.accumulator(0L)
    if (writeAhead)
      sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val ssc = new StreamingContext(sc, Seconds(3))
    if (writeAhead)
      ssc.checkpoint(optionMap(RecommendStreamingCommandParser.checkpoint))

    val topicMap = optionMap(RecommendStreamingCommandParser.topics).split(",").map((_, optionMap(RecommendStreamingCommandParser.numThreads).toInt)).toMap
    val kafkaStreaming = KafkaUtils.createStream(ssc, optionMap(RecommendStreamingCommandParser.zkQuorum), optionMap(RecommendStreamingCommandParser.group), topicMap).map(_._2)
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
              case "hotsale" => ViewHandler.handle(json)
              case "dpgl" => ViewHandler.handle(json)
              case "afpay" => logger.info(record)
              case "pcgl" => PCGLHandler.handler(json)
              case "gyl" => GYLHandler.handler(json)
              case _ => logger.error("wrong message type: " + msgType)
            }
          } catch {
            case e: Exception => logger.error(s"$record : $e")
            case e0: Throwable => logger.error(s"super error $record : $e0")
          }
        }
      }
      if (debug) {
        if (received < messageCounter.value) {
          logger.info(s"receive ${messageCounter.value} records")
          received = messageCounter.value
        }
      }

    }

    ssc.start()
    ssc.awaitTermination()
  }

  def setSparkLoggerLevel(level: String): Unit = {
    level match {
      case "trace" => Logger.getRootLogger.setLevel(Level.TRACE)
      case _ => Logger.getRootLogger.setLevel(Level.INFO)

    }

  }

}


object RecommendStreamingCommandParser  {

  val seconds = "seconds"
  val zkQuorum = "zk.quorum"
  val group = "group"
  val topics = "topics"
  val numThreads = "numThreads"
  val writeAheadLog = "writeAheadLog"
  val checkpoint = "checkpoint"
  val debug = "debug"
  val log = "logLevel"

  val options = new Options
  val commandLineParser = new MyCommandLine("streaming")

  commandLineParser.addOption("h", "help", false, "print help information")
  commandLineParser.addOption("s", seconds, true, "spark streaming interval", "3")
  commandLineParser.addOption("t", topics, true, "consumer topics in kafka", true)
  commandLineParser.addOption("zk", zkQuorum, true, "zookeeper quorum", true)
  commandLineParser.addOption("g", group, true, "kafka consumer group", "streaming")
  commandLineParser.addOption("n", numThreads, true, "threads number to receive from kafka", "2")
  commandLineParser.addOption("w", writeAheadLog, true, "turn on spark streaming write ahead log", "true")
  commandLineParser.addOption("c", checkpoint, true, "spark streaming checkpoint path", "checkpoint/spark/recommend")
  commandLineParser.addOption("d", debug, true, "print received records number", "false")
  commandLineParser.addOption("l", log, true, " spark log level", "WARN")

  def parse(args: Array[String]): Map[String, String] = {
    commandLineParser.parser(args)
  }

  def printHelper = {
    val helpFormatter = new HelpFormatter
    helpFormatter.printHelp("recommend-streaming", options)
  }

}
