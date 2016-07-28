package com.bl.bigdata.streaming.mykafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

/**
 * Created by MK33 on 2016/6/12.
 */
object ConsumerDemo {

  def main(args: Array[String]) {
    if (args.length != 3) {
      println("please input <host> <group-id> and <topic>.")
      sys.exit(-1)
    }
    val Array(host, groupID, topic) = args
    val props = new Properties()
    props.put("bootstrap.servers", "10.201.129.74:9092")
    props.put("group.id", "test")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("test00"))
    while (true)
    {
      val records = consumer.poll(100000000L).iterator()
      while (records.hasNext){
        val record = records.next()
        println(s"topic: ${record.topic()}, partition: ${record.partition()}, offset: ${record.offset()}, key: ${record.key()}, value: ${record.value()}")
        Thread.sleep(1000)
      }
    }


  }

}
