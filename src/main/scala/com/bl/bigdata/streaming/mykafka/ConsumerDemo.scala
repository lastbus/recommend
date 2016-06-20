package com.bl.bigdata.streaming.mykafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

/**
 * Created by MK33 on 2016/6/12.
 */
object ConsumerDemo {

  def main(args: Array[String]) {
    val props = new Properties()
    props.put("bootstrap.servers", "10.201.129.75:9092")
    props.put("group.id", "demo-0")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("my-replicate-topic"))
    while (true)
    {
      val records = consumer.poll(10000).iterator()
      while (records.hasNext){
        val record = records.next()
        println(s"offset: ${record.offset()}, key: ${record.key()}, value: ${record.value()}")
      }

    }


  }

}
