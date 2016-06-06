package com.bl.bigdata.kafka

import java.util.Properties
import java.util.concurrent.{Executors, ExecutorService}

import kafka.consumer.{Consumer, ConsumerConfig}

import scala.collection.mutable

/**
 * Created by MK33 on 2016/6/2.
 */
class ConsumerGroupExample(val a_zookeeper: String, val a_groupId: String, val a_topic: String) {

  private val consumer = Consumer.create(createConsumerConfig)
  private val topic = a_topic
  private var executor: ExecutorService = _

  def shutdown = {

  }


  def run(a_numThreads: Int): Unit ={
    val topicCountMap = mutable.Map[String, Int]()
    topicCountMap.put(topic, a_numThreads)

    val consumerMap = consumer.createMessageStreams(topicCountMap)
    val streams = consumerMap.get(topic)
    executor = Executors.newFixedThreadPool(a_numThreads)



  }































  def createConsumerConfig: ConsumerConfig =
  {
    val props = new Properties()
    props.put("zookeeper.connect", a_zookeeper)
    props.put("group.id", a_groupId)
    new ConsumerConfig(props)
  }

}

object ConsumerGroupExample {

  def main(args: Array[String]) {
    val zooKeeper = args(0)
    val groupId = args(1)
    val topic = args(2)
    val threads = args(3).toInt
    val consumerGroupExample = new ConsumerGroupExample(zooKeeper, groupId, topic)

  }
}
