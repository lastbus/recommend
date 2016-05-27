package com.bl.bigdata.kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{OffsetAndMetadata, ConsumerRecord, KafkaConsumer}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by MK33 on 2016/5/24.
 */
object ManualConsumer {

  def main(args: Array[String]) {

    fineControl()

  }

  def coarseControl(): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "10.201.129.74:9092,10.201.129.75:9092,10.201.129.81:9092")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "false")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") // 如何将 byte 转换为 object
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("my-replicated-topic"))
    val minBatchSize = 10
    val list = new ArrayBuffer[ConsumerRecord[String, String]]()

    while (true)
    {
      val records = consumer.poll(1000)
      val iterator = records.iterator()
      while (iterator.hasNext)
        list += iterator.next()
      if (list.size > minBatchSize)
      {
        for (l <- list) println(s"offset = ${l.offset()} , key = ${l.key()} , value = ${l.value()}")
        consumer.commitSync()
        list.clear()
      }
    }
  }

  def fineControl(): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "10.201.129.74:9092,10.201.129.75:9092,10.201.129.81:9092")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "false")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") // 如何将 byte 转换为 object
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("my-replicated-topic"))

    try {
      while (true)
      {
        val records = consumer.poll(Long.MaxValue)
        val partitions = records.partitions().iterator()
        while (partitions.hasNext)
        {
          val partition = partitions.next()
          val partitionRecord = records.records(partition)
          val iterator = partitionRecord.iterator()
          while (iterator.hasNext)
          {
            val record = iterator.next()
            println(s"${record.offset()}, ${record.key()}, ${record.value()}")
          }
          val lastOffset = partitionRecord.get(partitionRecord.size() - 1).offset()
          consumer.commitSync(util.Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)))

        }

      }
    } catch {
      case e: Exception =>
        println(e.getMessage)
    } finally {
      consumer.close()
    }

  }

  def printDetailConsumerInfo = {
    val props = new Properties()
    props.put("bootstrap.servers", "10.201.129.74:9092,10.201.129.75:9092,10.201.129.81:9092")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "false")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") // 如何将 byte 转换为 object
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList("test3"))

    val consumerPartition = consumer.poll(Long.MaxValue)
    consumerPartition.partitions()

  }


}
