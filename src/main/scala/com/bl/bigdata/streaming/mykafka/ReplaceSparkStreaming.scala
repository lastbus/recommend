package com.bl.bigdata.streaming.mykafka

import java.util.Properties

import com.bl.bigdata.streaming.{GYLHandler, PCGLHandler, ViewHandler}
import com.bl.bigdata.util.MyCommandLine
import org.apache.commons.cli.{HelpFormatter, Options}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.logging.log4j.LogManager
import org.json.JSONObject

/**
  * Created by MK33 on 2016/9/13.
  */
object ReplaceSparkStreaming {

  val logger = LogManager.getLogger(this.getClass.getName)

  def main(args: Array[String]) {

    println("====  program begin  ====")

    // load kafka configuration file
    val classLoader = this.getClass.getClassLoader
    val kafkaProps = new Properties()
    val in2 = classLoader.getResourceAsStream("kafka2.properties")
    if (in2 != null) kafkaProps.load(in2) else {println("no kafka2.properties found!"); System.exit(-1)}

    val consumer = new KafkaConsumer[String, String](kafkaProps)
    consumer.subscribe(java.util.Arrays.asList("recommend_track"))

    sys.addShutdownHook(new Runnable {
      override def run(): Unit = {
        println("=====  begin to close kafka consumer  =====")
        consumer.close()
        println("=====  succeed to closed kafka consumer ===")
      }
    })

    while (true) {
      val records = consumer.poll(1000).iterator()
      while (records.hasNext){
        val record = records.next()
        try {
          val json = new JSONObject(record.value().toString)
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
          case e: Exception => logger.error(s"${record.value()} : $e")
          case e0: Throwable => logger.error(s"super error $record : $e0")
        }

//        println(record.value())
      }

    }

  }

}

