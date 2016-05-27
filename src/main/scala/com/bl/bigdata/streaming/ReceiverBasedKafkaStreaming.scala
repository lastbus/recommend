package com.bl.bigdata.streaming

import com.bl.bigdata.util.RedisClient
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject

import scala.collection.JavaConversions._

/**
 * Created by MK33 on 2016/5/26.
 */
object ReceiverBasedKafkaStreaming extends StreamingLogger {

  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: ReceiverBasedKafkaStreaming <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    logger.debug("=======   ReceiverBasedKafkaStreaming start  =========")

    val conf = new SparkConf().setAppName("receiver based")
    val ssc = new StreamingContext(conf, Seconds(3))
    val Array(zooKeeper, consumerGroup, topic, thread) = args

    val topics = topic.split(",").map((_, thread.toInt)).toMap
    val dstream = KafkaUtils.
      createStream(ssc, zooKeeper, consumerGroup, topics)
      .map(_._2)

    dstream.foreachRDD
    { rdd =>
      rdd.foreachPartition
      { partition =>
        val conn = HBaseConnectionPool.connection
        val table = conn.getTable(TableName.valueOf("kafka-streaming-test"))
        val jedis = RedisClient.pool.getResource
        partition.foreach { item =>
          try {
            val json = new JSONObject(item)
            val behaviorType = json.getString("actType")
            val cookie = json.getString("cookieId")
            val eventDate = json.getString("eventDate")
            val goodsId = json.getString("goodsId")
            val get = new Get(Bytes.toBytes(cookie))
            if (table.exists(get))
            {
//              val valueBytes = table.get(get).getValue(Bytes.toBytes("test"), Bytes.toBytes("cookie"))
//              val memberID = jedis.get(cookie)
              val memberID = new String(table.get(get).getValue(Bytes.toBytes("test"), Bytes.toBytes("cookie")))
              jedis.hmset("last_view_" + memberID, Map(goodsId -> eventDate))
              logger.debug(s"save to redis key: last_view_$memberID")
            } else {
              logger.debug(s"error: $cookie not found in hbase")
            }
          } catch {
            case e: Exception => logger.debug("not a json string: " + item)
          }
        }
        jedis.close()
      }


    }

    ssc.start()
    ssc.awaitTermination()

  }

}
