package com.bl.bigdata.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}

/**
 * Created by MK33 on 2016/5/30.
 */
object ProducerTest {

  def main(args: Array[String]) {

    val props = new Properties()
    props.put("bootstrap.servers", "10.201.48.103:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    for (i <- 0 to 10) {

      producer.send(new ProducerRecord[String, String]("test", null, "test"))

    }
    producer.flush()
    producer.close()


  }

}
