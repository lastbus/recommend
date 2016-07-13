package com.bl.bigdata.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.util.LoggerShutDown
import org.apache.commons.cli.{BasicParser, HelpFormatter, Option, Options}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

import scala.collection.mutable


/**
 * Created by MK33 on 2016/5/30.
 */
object RecommendStreaming extends StreamingLogger with LoggerShutDown {

  def main(args: Array[String]) {
    val optionMap = RecommendStreamingCommandParser.parse(args)
    println("program parameters:")
    for (key <- optionMap.keys){
      println(s"\t\t\t${key} = ${optionMap(key)}")
    }
    println(s"=========  begin to execute :  ${new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date())} =========")
//    val Array(zkQuorum, group, topics, numThreads) = args
    val writeAhead = optionMap(RecStreamingConf.writeAheadLog).toBoolean
    val debug = optionMap(RecStreamingConf.debug).toBoolean

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val messageCounter = sc.accumulator(0L)
    if (writeAhead)
      sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val ssc = new StreamingContext(sc, Seconds(3))
    if (writeAhead)
      ssc.checkpoint(optionMap(RecStreamingConf.checkpoint))

    val topicMap = optionMap(RecStreamingConf.topics).split(",").map((_, optionMap(RecStreamingConf.numThreads).toInt)).toMap
    val kafkaStreaming = KafkaUtils.createStream(ssc, optionMap(RecStreamingConf.zkQuorum), optionMap(RecStreamingConf.group), topicMap).map(_._2)
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

}


object RecommendStreamingCommandParser  {

  val options = new Options

  val help = new Option("h", "help", false, "print help information")
  val seconds  = new Option("s", RecStreamingConf.seconds, true, "spark streaming interval")
  val topics = new Option("t", RecStreamingConf.topics, true, "consumer topics in kafka")
  topics.setRequired(true)
  val zkQuorum = new Option("zk", RecStreamingConf.zkQuorum, true, "zookeeper quorum")
  zkQuorum.setRequired(true)
  val group = new Option("g", RecStreamingConf.group, true, "kafka consumer group")
  val numThreads = new Option("n", RecStreamingConf.numThreads, true, "threads number to receive from kafka")
  val writeAheadLog = new Option("w", RecStreamingConf.writeAheadLog, false, "turn on spark streaming write ahead log")
  val checkPoint = new Option("c", RecStreamingConf.checkpoint, true, "spark streaming checkpoint path")
  val debug = new Option("d", RecStreamingConf.debug, false, "print received records number")

  options.addOption(help)
  options.addOption(seconds)
  options.addOption(topics)
  options.addOption(zkQuorum)
  options.addOption(group)
  options.addOption(numThreads)
  options.addOption(writeAheadLog)
  options.addOption(checkPoint)
  options.addOption(debug)

  val basicParser = new BasicParser

  def parse(args: Array[String]): Map[String, String] = {
    val commandLine = try {
      basicParser.parse(options, args)
      } catch {
        case e: Exception =>
          println(e.getMessage)
          printHelper
          sys.exit(-1)
    }
    val optionMap = RecStreamingConf.optionMap

    if (commandLine.hasOption("help")){
      printHelper
    }
    if (commandLine.hasOption(RecStreamingConf.seconds)){
      optionMap(RecStreamingConf.seconds) = commandLine.getOptionValue(RecStreamingConf.seconds)
    }
    if (commandLine.hasOption(RecStreamingConf.zkQuorum)) {
      optionMap(RecStreamingConf.zkQuorum) = commandLine.getOptionValue(RecStreamingConf.zkQuorum)
    }
    if (commandLine.hasOption(RecStreamingConf.topics)){
      optionMap(RecStreamingConf.topics) = commandLine.getOptionValue(RecStreamingConf.topics)
    }
    if (commandLine.hasOption(RecStreamingConf.group)){
      optionMap(RecStreamingConf.group) = commandLine.getOptionValue(RecStreamingConf.group)
    }
    if (commandLine.hasOption(RecStreamingConf.numThreads))  {
      optionMap(RecStreamingConf.numThreads) = commandLine.getOptionValue(RecStreamingConf.numThreads)
    }
    if (commandLine.hasOption(RecStreamingConf.writeAheadLog))  {
      optionMap(RecStreamingConf.writeAheadLog) = commandLine.getOptionValue(RecStreamingConf.writeAheadLog)
    }
    if (commandLine.hasOption(RecStreamingConf.checkpoint)) {
      optionMap(RecStreamingConf.checkpoint) = commandLine.getOptionValue(RecStreamingConf.checkpoint)
    }
    if (commandLine.hasOption(RecStreamingConf.debug)) {
      optionMap(RecStreamingConf.debug) = "true"
    }

    optionMap.toMap
  }

  def printHelper = {
    val helpFormatter = new HelpFormatter
    helpFormatter.printHelp("recommend-streaming", options)
  }

}


object RecStreamingConf {

  val seconds = "seconds"

  val zkQuorum = "zk-quorum"
  val group = "group"
  val topics = "topics"
  val numThreads = "numThreads"

  val writeAheadLog = "writeAheadLog"
  val checkpoint = "checkpoint"

  val debug = "debug"

  val optionMap = mutable.Map(seconds -> "3", group -> "recommend-api-view", numThreads -> "2",
                              writeAheadLog -> "true", checkpoint -> "checkpoint/recommend-api-view", debug -> "false")

}
