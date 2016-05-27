package com.bl.bigdata.kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

/**
 * Created by MK33 on 2016/5/24.
 */
object AutomaticConsumer {

  def main(args: Array[String]) {
    val props = new Properties()
    props.put("bootstrap.servers", "10.201.129.74:9092,10.201.129.75:9092,10.201.129.81:9092")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") // 如何将 byte 转换为 object

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("my-replicated-topic"))

    while (true)
    {
      val records = consumer.poll(100)
      val iterator = records.iterator()
      while (iterator.hasNext)
      {
        val next = iterator.next()
        println(s"offset = ${next.offset()}, key = ${next.key()}, value = ${next.value()} .")
      }
    }


  }
}
