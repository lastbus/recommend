package com.bl.bigdata.streaming

import org.apache.commons.cli.{HelpFormatter, BasicParser, Options, Option}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

/**
 * Created by MK33 on 2016/6/29.
 */
object Window {

  def main(args: Array[String]) {

    Logger.getRootLogger.setLevel(Level.WARN)

    if (args.length < 4) {
      System.err.println("Usage: RecommendStreaming <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val messageCounter = sc.accumulator(0L)
    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("/user/spark/checkpoint/kafka/recommend2")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaStreaming = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    var received = 0L

    kafkaStreaming.map { s =>
      val json = new JSONObject(s)
      (if (json.getString("actType") == "view") json.getString("goodsId") else "not know", 1)
    }.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(3600), Seconds(4))
    .foreachRDD { rdd => rdd.top(20)(Ordering.by(_._2)).foreach(s => println("=======" + s)) }

    kafkaStreaming.map { s =>
      val json = new JSONObject(s)
      (if (json.getString("actType") == "view") json.getString("goodsId") else "not know", 1)
    }.reduceByKeyAndWindow((a: Int, b: Int) => a + b, (a: Int, b: Int) => a - b, Seconds(3600), Seconds(4))
    .foreachRDD { rdd => rdd.top(20)(Ordering.by(_._2)).foreach(s => println("-------" + s)) }


    ssc.start()
    ssc.awaitTermination()


  }




}


object HotSaleConf {
  // 热销商品的参数
  val interval = "interval"
  val windowLength = "window.length"
  val slideInterval = "slide.interval"
  val zkQuorum = "zk.quorum"
  val group = "group"
  val topic = "topic"
  val numberThreads = "num.threads"

  val optionMap = scala.collection.mutable.Map(interval -> "2",
                   windowLength -> "3600",
                   slideInterval -> "4",
                   zkQuorum -> "",
                   group -> "hot-sale-statistics",
                   topic -> "",
                   numberThreads -> "")

}

object HotSaleCommandParser {

  val options = new Options

  val helpOption = new Option("h", "help", false, "print help information")
  val intervalOption = new Option("i", HotSaleConf.interval, true, "spark streaming interval")
  intervalOption.setArgName("interval")
  val windowLengthOption = new Option("n", HotSaleConf.windowLength, true, "the number of windows in spark streaming")
  windowLengthOption.setArgName("windowLength")
  val slideIntervalOption = new Option("si", HotSaleConf.slideInterval, true, "the calculator interval of spark streaming")
  slideIntervalOption.setArgName("slideInterval")
  val zkQuorumOption = new Option("zk", HotSaleConf.zkQuorum, true, "zookeeper address")
  zkQuorumOption.setArgName("zkQuorum")
  zkQuorumOption.setRequired(true)
  val groupOption = new Option("g", HotSaleConf.group, true, "kafka consumer group")
  groupOption.setArgName("group")
  val topicOption = new Option("t", HotSaleConf.topic, true, "the subscribed kafka topic")
  topicOption.setArgName("topic")
  topicOption.setRequired(true)
  val numberThreadsOption = new Option("t", HotSaleConf.numberThreads, true, "the number of threads")
  numberThreadsOption.setArgName("threadsNumber")
  numberThreadsOption.setRequired(true)

  options.addOption(helpOption)
  options.addOption(intervalOption)
  options.addOption(windowLengthOption)
  options.addOption(slideIntervalOption)
  options.addOption(zkQuorumOption)
  options.addOption(groupOption)
  options.addOption(topicOption)
  options.addOption(numberThreadsOption)


  val commandParser = new BasicParser()

  def parser(args: Array[String]): Unit = {

    val commandLine =
      try {
      commandParser.parse(options, args)
    } catch {
      case e: Exception =>
        println(e.getMessage)
        printHelper()
        sys.exit(-1)
    }

      if (commandLine.hasOption("h") | commandLine.hasOption("help")) {
        printHelper()
        sys.exit(0)
      }

      if (commandLine.hasOption(HotSaleConf.interval)) {
        HotSaleConf.optionMap(HotSaleConf.interval) = commandLine.getOptionValue(HotSaleConf.interval)
        println(commandLine.getOptionValue(HotSaleConf.interval))
      }
      if (commandLine.hasOption(HotSaleConf.windowLength)) {
        HotSaleConf.optionMap(HotSaleConf.windowLength) = commandLine.getOptionValue(HotSaleConf.windowLength)
        println(commandLine.getOptionValue(HotSaleConf.windowLength))
      }
      if (commandLine.hasOption(HotSaleConf.slideInterval)) {
        HotSaleConf.optionMap(HotSaleConf.slideInterval) = commandLine.getOptionValue(HotSaleConf.slideInterval)
        println(commandLine.getOptionValue(HotSaleConf.slideInterval))
      }
      if (commandLine.hasOption(HotSaleConf.zkQuorum)) {
        HotSaleConf.optionMap(HotSaleConf.zkQuorum) = commandLine.getOptionValue(HotSaleConf.zkQuorum)
        println(commandLine.getOptionValue(HotSaleConf.zkQuorum))
      }
      if (commandLine.hasOption(HotSaleConf.group)) {
        HotSaleConf.optionMap(HotSaleConf.group) = commandLine.getOptionValue(HotSaleConf.group)
        println(commandLine.getOptionValue(HotSaleConf.group))
      }
      if (commandLine.hasOption(HotSaleConf.topic)) {
        HotSaleConf.optionMap(HotSaleConf.topic) = commandLine.getOptionValue(HotSaleConf.topic)
        println(commandLine.getOptionValue(HotSaleConf.topic))
      }
      if (commandLine.hasOption(HotSaleConf.numberThreads)) {
        HotSaleConf.optionMap(HotSaleConf.group) = commandLine.getOptionValue(HotSaleConf.numberThreads)
        println(commandLine.getOptionValue(HotSaleConf.numberThreads))
      }
    }




  def printHelper(): Unit = {
    val helperFormatter = new HelpFormatter
    helperFormatter.printHelp("HotSale", options)
  }

}

