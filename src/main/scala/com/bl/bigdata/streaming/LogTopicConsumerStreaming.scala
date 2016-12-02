package com.bl.bigdata.streaming

import java.util.Properties

import com.bl.bigdata.datasource.HBaseConnectionFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.{JSONException, JSONObject}

import scala.collection.JavaConversions._

/**
  * direct streaming don't remember topic's partition offset,
  * if you don't give it offset, then it will poll the latest records in topics by default.
  * Created by make on 10/31/16.
  */
object LogTopicConsumerStreaming {

  val logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val seconds = if (args.length > 0) args(0).toInt else 1
    //    Logger.getRootLogger.setLevel(Level.WARN)
    // load properties file
    val inputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("kafka.properties")
    if (inputStream == null) {
      logger.error(
        """
          |Cannot found kafka.properties file, please make sure this file is in your classpath.
          |
        """.stripMargin)
      sys.exit(-1)
    }
    val props = new Properties()
    props.load(inputStream)
    logger.info("kafka.properties: ")
    logger.info("-------------------")
    val properties = for ((k, v) <- props if (k != "topic")) yield {
      logger.info(k + ":" + v)
      (k, v)
    }
    logger.info("------------------")
    // kafka client properties
    val kafkaParams = properties.toMap
    // subscribe topics
    val topicSet = props.getProperty("topic").split(",").toSet
    val groupId = properties.get("group.id")
    logger.info("subscribe topic: " + topicSet)
    logger.info("consumer group id: " + groupId)

    require(topicSet.size > 0, "Please subscribe at least on topic !")

    if (groupId.isEmpty) {
      logger.error("missing group.id param in kafka.properties file")
      sys.exit(-1)
    }
    // find group consumer offset or initial its offset when first running
    val kafkaCluster = new KafkaCluster(kafkaParams)
    val topicAndPartitionEither = kafkaCluster.getPartitions(topicSet)
    // if cannot find topic set info, then exit.
    if (topicAndPartitionEither.isLeft) {
      logger.error("get kafka partition failed: " + topicSet + " , " + topicAndPartitionEither.left)
      sys.exit(-1)
    }
    val topicAndPartitionSet = topicAndPartitionEither.right.get
    val consumerOffset = kafkaCluster.getConsumerOffsets(groupId.get, topicAndPartitionSet)
    if (consumerOffset.isLeft) {
      logger.info(s"zookeeper cannot find group offset:  ${consumerOffset.left},  initialize consumer offset ")
      // begin to set the group consumer offset, default to the earliest
      val offsetRest = properties.getOrElse("auto.offset.reset", "smallest")
      val earliestLeaderOffset =
        if (offsetRest == "smallest")
          kafkaCluster.getEarliestLeaderOffsets(topicAndPartitionSet)
        else if (offsetRest == "largest")
          kafkaCluster.getLatestLeaderOffsets(topicAndPartitionSet)
        else
          kafkaCluster.getEarliestLeaderOffsets(topicAndPartitionSet)
      //      val earlistLeaderOffset = kafkaCluster.getEarliestLeaderOffsets(topicAndPartitionSet)

      if (earliestLeaderOffset.isLeft) {
        logger.error(s"topic: ${topicAndPartitionSet}, get earliest leader offset encounter error: " + topicAndPartitionSet)
        sys.exit(-1)
      } else {
        kafkaCluster.setConsumerOffsets(groupId.get, earliestLeaderOffset.right.get.mapValues(_.offset))
      }
    }

    val topicAndPartitionMap = kafkaCluster.getConsumerOffsets(groupId.get, topicAndPartitionSet).right.get

    val sparkConf = new SparkConf()

    //sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000") this can be set in command line
    val ssc = new StreamingContext(sparkConf, Seconds(seconds))
    logger.info("begin to get instance of kafka direct streaming")
    val message =  KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, Int, Long, String, String)](
      ssc, kafkaParams, topicAndPartitionMap, (mm: MessageAndMetadata[String, String]) => (mm.topic, mm.partition, mm.offset, mm.key(),mm.message()))


    var offsetRanges = Array[OffsetRange]()
    val receiveRawBytes = Bytes.toBytes("json")
    val columnFamilyBytes = Bytes.toBytes("info")
    val createDateBytes = Bytes.toBytes("create_date")
    val memberIdBytes = Bytes.toBytes("member_id")
    val memberLantitudeBytes = Bytes.toBytes("member_latitude")
    val memberLongitudeBytes = Bytes.toBytes("member_longitude")
    val methodBytes = Bytes.toBytes("method")
    val methodTypeBytes = Bytes.toBytes("method_type")


    message.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { rdd =>
      // there are more than one records in a seconds, so zip records with unique id to identify them
      rdd.zipWithUniqueId().foreachPartition { partition =>

        val hTable = HBaseConnectionFactory.getConnection("real_time_lat_lng")
        //        val kafkaClusterConn = KafkaConnectionFactory.getKafkaCluster
        val puts =
          partition.map { case ((topic, partition, offset, k, v), index) =>
            logger.debug(s"topic:$topic, partition: $partition, offset: $offset, key: $k, value: $v")
            try {
              val json = new JSONObject(v)
              val createDate = json.getString("createDate")
              val memberId = json.getString("memberId")
              val memberLantitude = json.getString("memberLantitude")
              val memberLongitude = json.getString("memberLongitude")
              val method = json.getString("method")
              val methodType = json.getString("methodType")
              val put = new Put(Bytes.toBytes(s"$createDate|$memberId|$methodType|$index"))
              put.addColumn(columnFamilyBytes, receiveRawBytes, Bytes.toBytes(v))
              put.addColumn(columnFamilyBytes, createDateBytes, Bytes.toBytes(createDate))
              put.addColumn(columnFamilyBytes, memberIdBytes, Bytes.toBytes(memberId))
              put.addColumn(columnFamilyBytes, memberLantitudeBytes, Bytes.toBytes(memberLantitude))
              put.addColumn(columnFamilyBytes, memberLongitudeBytes, Bytes.toBytes(memberLongitude))
              put.addColumn(columnFamilyBytes, methodBytes, Bytes.toBytes(method))
              put.addColumn(columnFamilyBytes, methodTypeBytes, Bytes.toBytes(methodType))
              Some(put)
            } catch {
              case e: JSONException =>
                logger.error("JSONException: " + e)
                None
              case e: Throwable => throw e
            }

          }.filter(_.isDefined).map(_.get).toList
        try {
          hTable.put(puts)
        } catch {
          case e: Throwable => throw e
        }

      }

      // this code block is executing on local machine
      offsetRanges.foreach { offsetRanges =>
        logger.info(
          s"""
             |topic: ${offsetRanges.topic}, partition: ${offsetRanges.partition}, fromOffset: ${offsetRanges.fromOffset}, untilOffset: ${offsetRanges.untilOffset}
          """.stripMargin)
      }

      val update = kafkaCluster.setConsumerOffsets(groupId.get, offsetRanges.map(s => (TopicAndPartition(s.topic, s.partition), s.untilOffset)).toMap)
      if (update.isLeft) {
        logger.error(s"update topic '${offsetRanges.toString()}' encounter error: " + update.left)
      }
      // update the topic offset when a batch finished
      //      val updateInfo = offsetRanges.map(s => (new TopicAndPartition(s.topic, s.partition), s.untilOffset)).toMap
      //      kafkaCluster.setConsumerOffsets(groupId.get, updateInfo)

    }

    ssc.start()
    ssc.awaitTermination()


  }

}
