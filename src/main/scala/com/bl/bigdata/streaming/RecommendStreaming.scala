package com.bl.bigdata.streaming

import com.bl.bigdata.util.{LoggerShutDown, RedisClient}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._
import scala.util.parsing.json.{JSON, JSONObject}

/**
 * Created by MK33 on 2016/5/30.
 */
object RecommendStreaming extends StreamingLogger with LoggerShutDown {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("recommendation streaming")
    sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("/user/spark/checkpoint/kafka/recommend")

    val zooKeeperConnection = "s103.pre.bl.bigdata:2180,s103.pre.bl.bigdata:2180,s103.pre.bl.bigdata:2180"
    val group = "recommend-real-time"
    val topic = "recommend_track"

    val kafkaStreaming = KafkaUtils.createStream(ssc, zooKeeperConnection, group, Map(topic -> 2)).map(_._2)

    val columnFamilyBytes = Bytes.toBytes("member_id")

    kafkaStreaming.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        val hBaseConn = HBaseConnectionPool.connection
        val hTable = hBaseConn.getTable(TableName.valueOf("member_cookie_mapping"))
        val jedis = RedisClient.pool.getResource
        partition.foreach { record =>
          logger.debug(record)
          try {
            val json: Map[String, _] = JSON.parseRaw(record).get.asInstanceOf[JSONObject].obj
            val msgType = json("actType").toString
            msgType match {
              case "view" =>
                val cookieID = json("cookieId").toString
                val get = new Get(Bytes.toBytes(cookieID))
                if (hTable.exists(get)) {
                  val memberIDBytes = hTable.get(get).getValue(columnFamilyBytes, columnFamilyBytes)
                  val memberId = new String(memberIDBytes)
                  logger.debug(json("eventDate").toString)
                  val map = Map(json("eventDate").toString -> json("goodsId").toString)
                  jedis.hmset("rcmd_rt_view_" + memberId, map)
                  jedis.expire("rcmd_rt_view_" + memberId, 3600 * 10)
                  logger.debug(s"redis key: rcmd_rt_view_$memberId, value: $map")
                } else {
                  logger.info(s"$cookieID not found in hbase")
                }
              case _ => logger.error("wrong message type: " + msgType)
            }
          } catch {
            case e: Exception => logger.error(s"$record : ${e.getMessage}")
            case e0 => logger.error(s"super error $record : ${e0.getMessage}")
          }
        }
        hTable.close()
        jedis.close()
      }

    }
    ssc.start()
    ssc.awaitTermination()
  }

}
