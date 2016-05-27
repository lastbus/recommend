package com.bl.bigdata.streaming

import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * Created by MK33 on 2016/5/23.
 */
object KafkaStreaming {
  val path = "/tmp/spark-streaming-kafka-test"

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val sparkconf = new SparkConf().setAppName("kafka-test")
    val ssc = new StreamingContext(sparkconf, Seconds(2))
    ssc.checkpoint(path)
    val props = new Properties()
    props.load(new BufferedInputStream(new FileInputStream("producer-demo.properties")))
    val map = mutable.Map.empty[String, String]
//    for ((k, v) <- props.entrySet()) map += (k, v)
//    for (k <- props.keySet()) map(k) = props.getProperty(k)
    val kvs = props.keySet().toArray()
    for (k <- kvs) map(k.toString) = props.getProperty(k.toString)
    println("===============")
    map.foreach(println)
    println("------------")
    val directKafkaStream = KafkaUtils.createDirectStream(ssc,
                                                          map.toMap,
                                                          Set("my-replicated-topic"))


//      [StringSerializer, StringSerializer, StringDeserializer, StringDeserializer]

    val word = directKafkaStream.map(_._2.toString)
//                                .map((_, 1)).reduceByKey(_ + _)
//                                .updateStateByKey(updateFunction _)

    word.print()

    ssc.start()
    ssc.awaitTermination()


  }

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newValue = runningCount.getOrElse(0) + newValues.sum
    Option(newValue)
  }

}
