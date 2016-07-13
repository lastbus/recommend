package com.bl.bigdata.streaming.mykafka

import java.util.Properties

import org.apache.kafka.clients.producer.{RecordMetadata, ProducerRecord, KafkaProducer, Callback}

/**
 * Created by MK33 on 2016/6/12.
 */
object ProducerDemo {

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("please input kafka server url:port and topic")
      sys.exit(-1)
    }
    val Array(url, topic) = args
    val props = new Properties()
    props.put("bootstrap.servers", url)
    props.put("client.id", "DemoProducer")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    for (i <- 0 to 2000000000){
      try {
        val metadata = producer.send(new ProducerRecord[String, String](topic, null, "foo" + i.toString),
        new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if (metadata != null ) println("topic: " + metadata.topic() + "   partition:  " + metadata.partition() + "  offset: " + metadata.offset() + "   value: ")
            if (exception != null) println(exception.getMessage)
          }
        })
      } catch {
        case e: Throwable => println(e.getMessage)
      }
      println("send " + i + " to " + url + " : " + topic)
      Thread.sleep(1000)
    }

    producer.close()
  }

}
