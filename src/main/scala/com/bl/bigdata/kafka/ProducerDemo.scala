package com.bl.bigdata.kafka

import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}

/**
 * Created by MK33 on 2016/5/23.
 */
object ProducerDemo {

  def main(args: Array[String]) {

    if (args.length < 1)
    {
      println("please input the <properties file name> <kafka topic>")
      sys.exit(0)
    }
    val Array(propertiesPath, topic) = args
    val props = new Properties()
    props.load(new BufferedInputStream(new FileInputStream(propertiesPath)))

    val producer = new KafkaProducer[String, String](props)
    var key = 0L
    try {
      while (true)
      {
        val message = new ProducerRecord[String, String](topic, key.toString, key.toString)
        producer.send(message)
        key += 1
        Thread.sleep(500)
      }
    } catch {
      case e: Exception => print(e.getMessage)
    } finally {
      producer.close()
    }

  }


}
