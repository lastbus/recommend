package com.bl.bigdata.streaming.mykafka

import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}

import scala.collection.mutable

/**
 *
 * high level consumer example
 * Created by MK33 on 2016/6/15.
 */
object ConsumerGroupExample {

  def main(args: Array[String]): Unit = {

    val Array(zookeeper, groupId, topic, threads) = args
    val example = new ConsumerGroupExample(zookeeper, groupId, topic)
    example.run(threads.toInt)
    try {
      Thread.sleep(10000)
    } catch {
      case e: InterruptedException => println("interrupted!")
    }
//    example.shutdown()
  }

}

class ConsumerGroupExample(val a_zookeeper: String, val a_group: String, val a_topic: String) {

  val consumer = Consumer.create(createConsumerConfig(a_zookeeper, a_group))
  var executor: ExecutorService = _

  private def createConsumerConfig(a_zookeeper: String, a_groupId: String): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", a_zookeeper)
    props.put("group.id", a_groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    new ConsumerConfig(props)
  }

  def shutdown(): Unit = {
    if (consumer != null) consumer.shutdown()
    if (executor != null) executor.shutdown()
    try {
      if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)){
        println("Timed out waiting for consumer threads to shut down, exiting uncleanly")
      }
    } catch {
      case e: InterruptedException => println("Interrupted during shutdown, exiting uncleanly")
    }
  }

  def run(numThreads: Int): Unit = {
    val topicCountMap = mutable.Map[String, Int]()
    topicCountMap.put(a_topic, numThreads)
    val consumerMap = consumer.createMessageStreams(topicCountMap)
    val streams = consumerMap(a_topic)
    executor = Executors.newFixedThreadPool(numThreads)
    var threadNumber = 0
    streams.foreach { stream =>
      executor.submit(new ConsumerTest(stream, numThreads))
      threadNumber += 1
    }
  }

}


class ConsumerTest(val stream: KafkaStream[Array[Byte], Array[Byte]], val threadNumber: Int) extends Runnable {
  override def run(): Unit = {
    val it= stream.iterator()
    while (it.hasNext()){
      val msgMetaData = it.next()
      println("Thread " + threadNumber + ": " +
        "  topic : " + new String(msgMetaData.topic) +
        "  partition : " + msgMetaData.partition +
      "  offset : " + msgMetaData.offset +
      "  message : " + new String(msgMetaData.message()))
    }
    println("Shutting down Thread: " + threadNumber)
  }
}




