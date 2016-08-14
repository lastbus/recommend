package com.bl.bigdata.streaming

import java.util.Properties

import com.bl.bigdata.streaming.KafkaCluster.LeaderOffset
import com.bl.bigdata.util.MyCommandLine
import kafka.api.{OffsetCommitRequest, OffsetFetchRequest, PartitionOffsetRequestInfo, PartitionOffsetsResponse, _}
import kafka.common.{ErrorMapping, OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.commons.cli.{HelpFormatter, Options}
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.NonFatal

/**
  * Created by MK33 on 2016/8/2.
  */
object DirectStreamingTest {

  val logger = LogManager.getLogger(this.getClass.getName)

  def main(args: Array[String]) {

    val optionMap = DirectStreamingConf.parse(args)

    println("==========  DirectStreamingTest program start!  ==========")
    //    LogManager.getRootLogger.setLevel(Level.WARN)
    val classLoader = this.getClass.getClassLoader
    val kafkaProps = new Properties()
    val in2 = classLoader.getResourceAsStream("kafka.properties")
    if (in2 != null) kafkaProps.load(in2) else {println("no kafka.properties found!"); System.exit(-1)}

    val kafkaParams = mutable.Map[String, String]()
    val keys = kafkaProps.keySet().iterator()
    while (keys.hasNext) {
      val key = keys.next().toString
      kafkaParams(key) = kafkaProps.getProperty(key)
    }
    if (kafkaParams.size == 0) {
      println("no kafka properties found in kafka.properties")
      sys.exit(-1)
    }

    println("\noutput kafka properties")
    kafkaParams.foreach { case (k, v) =>
        println(k + ":  " + v)
    }
    println()

    val topics = optionMap(DirectStreamingConf.topics).split(",").toSet
    val seconds = optionMap(DirectStreamingConf.seconds).toInt
    val debug = optionMap(DirectStreamingConf.debug).toBoolean

    val ssc = new StreamingContext(new SparkConf(), Seconds(seconds))

    val kafkaManager = new KafkaManager(kafkaParams.toMap)

    val directKafkaStream = kafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams.toMap, topics)
    var offsetRanges = Array[OffsetRange]()
    // record how many records received
    val messageCounter = ssc.sparkContext.accumulator(0L)
    var received = 0L

    directKafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { rdd =>
      for (o <- offsetRanges)
        println(s"${o.topic}  ${o.partition}  ${o.fromOffset}  ${o.untilOffset}")

      rdd.map(_._2).foreachPartition { partition =>
        partition.foreach { record =>
          logger.debug(record)
          try {
            messageCounter += 1
            val json = new JSONObject(record)
            val msgType = json.getString("actType").toLowerCase
            msgType match {
              case "hotsale" => ViewHandler.handle(json)
              case "dpgl" => ViewHandler.handle(json)
              case "afpay" => logger.info(record)
              case "pcgl" => PCGLHandler.handler(json)
              case "gyl" => GYLHandler.handler(json)
              case _ => logger.error("wrong message type: " + msgType)
            }
          } catch {
            case e: Exception => logger.error(s"$record : $e")
            case e0: Throwable => logger.error(s"super error $record : $e0")
          }
        }
      }
      // 更新 kafka offset
      kafkaManager.updateZKOffsets(rdd)
      // 调试，是否打印出接收数据的条数
      if (debug) {
        if (received < messageCounter.value) {
          logger.info(s"receive ${messageCounter.value} records")
          received = messageCounter.value
        }
      }

    }

    sys.addShutdownHook(new Runnable {
      override def run(): Unit = {
        logger.info("############## stopping StreamingContext ########################")
        ssc.stop()
        logger.info("############## stopped StreamingContext  ########################")
      }
    })
    ssc.start()
    ssc.awaitTermination()


  }
}

object DirectStreamingConf  {

  val seconds = "seconds"
  val zkQuorum = "zk.quorum"
  val group = "group"
  val topics = "topics"
  val numThreads = "numThreads"
  val writeAheadLog = "writeAheadLog"
  val checkpoint = "checkpoint"
  val debug = "debug"
  val log = "logLevel"

  val options = new Options
  val commandLineParser = new MyCommandLine("streaming")

  commandLineParser.addOption("h", "help", false, "print help information")
  commandLineParser.addOption("s", seconds, true, "spark streaming interval", "1")
  commandLineParser.addOption("t", topics, true, "consumer topics in kafka", true)
  commandLineParser.addOption("zk", zkQuorum, true, "zookeeper quorum", true)
  commandLineParser.addOption("g", group, true, "kafka consumer group", "streaming")
//  commandLineParser.addOption("c", checkpoint, true, "spark streaming checkpoint path", true)
  commandLineParser.addOption("d", debug, true, "print received records number", "false")
  commandLineParser.addOption("l", log, true, " spark log level", "WARN")

  def parse(args: Array[String]): Map[String, String] = {
    commandLineParser.parser(args)
  }

  def printHelper = {
    val helpFormatter = new HelpFormatter
    helpFormatter.printHelp("recommend-streaming", options)
  }

}


private[streaming]
class KafkaManager(val kafkaParams: Map[String, String]) extends Serializable {

  val kc = new KafkaCluster(kafkaParams)

  /**
    * 创建数据流
    * @param ssc
    * @param kafkaParams
    * @param topics
    * @return
    */
  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag](
       ssc: StreamingContext, kafkaParams: Map[String, String], topics: Set[String]): InputDStream[(K, V)] =  {
    val groupId = kafkaParams.get("group.id").get
    // 在zookeeper上读取offsets前先根据实际情况更新offsets
    setOrUpdateOffsets(topics, groupId)

    //从zookeeper上读取offset开始消费message
    val messages = {
      val partitionsE = kc.getPartitions(topics)
      if (partitionsE.isLeft)
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
      val partitions = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft)
        throw new SparkException(s"get kafka consumer offsets failed: ${consumerOffsetsE.left.get}")
      val consumerOffsets = consumerOffsetsE.right.get
      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](
        ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message))
    }
    messages
  }

  /**
    * 创建数据流前，根据实际消费情况更新消费offsets
    * @param topics
    * @param groupId
    */
  private def setOrUpdateOffsets(topics: Set[String], groupId: String): Unit = {
    topics.foreach(topic => {
      var hasConsumed = true
      val partitionsE = kc.getPartitions(Set(topic))
      if (partitionsE.isLeft)
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
      val partitions = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft) hasConsumed = false
      if (hasConsumed) {// 消费过
        /**
          * 如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，
          * 说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
          * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
          * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
          * 这时把consumerOffsets更新为earliestLeaderOffsets
          */
        val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
        if (earliestLeaderOffsetsE.isLeft)
          throw new SparkException(s"get earliest leader offsets failed: ${earliestLeaderOffsetsE.left.get}")
        val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
        val consumerOffsets = consumerOffsetsE.right.get

        // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
        var offsets: Map[TopicAndPartition, Long] = Map()
        consumerOffsets.foreach({ case(tp, n) =>
          val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
          if (n < earliestLeaderOffset) {
            println("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +
              " offsets已经过时，更新为" + earliestLeaderOffset)
            offsets += (tp -> earliestLeaderOffset)
          }
        })
        if (!offsets.isEmpty) {
          kc.setConsumerOffsets(groupId, offsets)
        }
      } else {// 没有消费过
      val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
        var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
        if (reset == Some("smallest")) {
          val leaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft)
            throw new SparkException(s"get earliest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        } else {
          val leaderOffsetsE = kc.getLatestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft)
            throw new SparkException(s"get latest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        }
        val offsets = leaderOffsets.map {
          case (tp, offset) => (tp, offset.offset)
        }
        kc.setConsumerOffsets(groupId, offsets)
      }
    })
  }

  /**
    * 更新zookeeper上的消费offsets
    * @param rdd
    */
  def updateZKOffsets(rdd: RDD[(String, String)]) : Unit = {
    val groupId = kafkaParams.get("group.id").get
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
      if (o.isLeft) {
        println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      }
    }
  }
}

private[streaming]
class KafkaCluster(val kafkaParams: Map[String, String]) extends Serializable {
  import KafkaCluster.{Err, LeaderOffset, SimpleConsumerConfig}

  // ConsumerConfig isn't serializable
  @transient private var _config: SimpleConsumerConfig = null

  def config: SimpleConsumerConfig = this.synchronized {
    if (_config == null) {
      _config = SimpleConsumerConfig(kafkaParams)
    }
    _config
  }

  def connect(host: String, port: Int): SimpleConsumer =
    new SimpleConsumer(host, port, config.socketTimeoutMs,
      config.socketReceiveBufferBytes, config.clientId)

  def connectLeader(topic: String, partition: Int): Either[Err, SimpleConsumer] =
    findLeader(topic, partition).right.map(hp => connect(hp._1, hp._2))

  // Metadata api
  // scalastyle:off
  // https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-MetadataAPI
  // scalastyle:on

  def findLeader(topic: String, partition: Int): Either[Err, (String, Int)] = {
    val req = TopicMetadataRequest(TopicMetadataRequest.CurrentVersion,
      0, config.clientId, Seq(topic))
    val errs = new Err
    withBrokers(Random.shuffle(config.seedBrokers), errs) { consumer =>
      val resp: TopicMetadataResponse = consumer.send(req)
      resp.topicsMetadata.find(_.topic == topic).flatMap { tm: TopicMetadata =>
        tm.partitionsMetadata.find(_.partitionId == partition)
      }.foreach { pm: PartitionMetadata =>
        pm.leader.foreach { leader =>
          return Right((leader.host, leader.port))
        }
      }
    }
    Left(errs)
  }

  def findLeaders(
                   topicAndPartitions: Set[TopicAndPartition]
                 ): Either[Err, Map[TopicAndPartition, (String, Int)]] = {
    val topics = topicAndPartitions.map(_.topic)
    val response = getPartitionMetadata(topics).right
    val answer = response.flatMap { tms: Set[TopicMetadata] =>
      val leaderMap = tms.flatMap { tm: TopicMetadata =>
        tm.partitionsMetadata.flatMap { pm: PartitionMetadata =>
          val tp = TopicAndPartition(tm.topic, pm.partitionId)
          if (topicAndPartitions(tp)) {
            pm.leader.map { l =>
              tp -> (l.host -> l.port)
            }
          } else {
            None
          }
        }
      }.toMap

      if (leaderMap.keys.size == topicAndPartitions.size) {
        Right(leaderMap)
      } else {
        val missing = topicAndPartitions.diff(leaderMap.keySet)
        val err = new Err
        err.append(new SparkException(s"Couldn't find leaders for ${missing}"))
        Left(err)
      }
    }
    answer
  }

  def getPartitions(topics: Set[String]): Either[Err, Set[TopicAndPartition]] = {
    getPartitionMetadata(topics).right.map { r =>
      r.flatMap { tm: TopicMetadata =>
        tm.partitionsMetadata.map { pm: PartitionMetadata =>
          TopicAndPartition(tm.topic, pm.partitionId)
        }
      }
    }
  }

  def getPartitionMetadata(topics: Set[String]): Either[Err, Set[TopicMetadata]] = {
    val req = TopicMetadataRequest(
      TopicMetadataRequest.CurrentVersion, 0, config.clientId, topics.toSeq)
    val errs = new Err
    withBrokers(Random.shuffle(config.seedBrokers), errs) { consumer =>
      val resp: TopicMetadataResponse = consumer.send(req)
      // error codes here indicate missing / just created topic,
      // repeating on a different broker wont be useful
      return Right(resp.topicsMetadata.toSet)
    }
    Left(errs)
  }

  // Leader offset api
  // scalastyle:off
  // https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetAPI
  // scalastyle:on

  def getLatestLeaderOffsets(
                              topicAndPartitions: Set[TopicAndPartition]
                            ): Either[Err, Map[TopicAndPartition, LeaderOffset]] =
    getLeaderOffsets(topicAndPartitions, OffsetRequest.LatestTime)

  def getEarliestLeaderOffsets(
                                topicAndPartitions: Set[TopicAndPartition]
                              ): Either[Err, Map[TopicAndPartition, LeaderOffset]] =
    getLeaderOffsets(topicAndPartitions, OffsetRequest.EarliestTime)

  def getLeaderOffsets(
                        topicAndPartitions: Set[TopicAndPartition],
                        before: Long
                      ): Either[Err, Map[TopicAndPartition, LeaderOffset]] = {
    getLeaderOffsets(topicAndPartitions, before, 1).right.map { r =>
      r.map { kv =>
        // mapValues isnt serializable, see SI-7005
        kv._1 -> kv._2.head
      }
    }
  }

  private def flip[K, V](m: Map[K, V]): Map[V, Seq[K]] =
    m.groupBy(_._2).map { kv =>
      kv._1 -> kv._2.keys.toSeq
    }

  def getLeaderOffsets(
                        topicAndPartitions: Set[TopicAndPartition],
                        before: Long,
                        maxNumOffsets: Int
                      ): Either[Err, Map[TopicAndPartition, Seq[LeaderOffset]]] = {
    findLeaders(topicAndPartitions).right.flatMap { tpToLeader =>
      val leaderToTp: Map[(String, Int), Seq[TopicAndPartition]] = flip(tpToLeader)
      val leaders = leaderToTp.keys
      var result = Map[TopicAndPartition, Seq[LeaderOffset]]()
      val errs = new Err
      withBrokers(leaders, errs) { consumer =>
        val partitionsToGetOffsets: Seq[TopicAndPartition] =
          leaderToTp((consumer.host, consumer.port))
        val reqMap = partitionsToGetOffsets.map { tp: TopicAndPartition =>
          tp -> PartitionOffsetRequestInfo(before, maxNumOffsets)
        }.toMap
        val req = OffsetRequest(reqMap)
        val resp = consumer.getOffsetsBefore(req)
        val respMap = resp.partitionErrorAndOffsets
        partitionsToGetOffsets.foreach { tp: TopicAndPartition =>
          respMap.get(tp).foreach { por: PartitionOffsetsResponse =>
            if (por.error == ErrorMapping.NoError) {
              if (por.offsets.nonEmpty) {
                result += tp -> por.offsets.map { off =>
                  LeaderOffset(consumer.host, consumer.port, off)
                }
              } else {
                errs.append(new SparkException(
                  s"Empty offsets for ${tp}, is ${before} before log beginning?"))
              }
            } else {
              errs.append(ErrorMapping.exceptionFor(por.error))
            }
          }
        }
        if (result.keys.size == topicAndPartitions.size) {
          return Right(result)
        }
      }
      val missing = topicAndPartitions.diff(result.keySet)
      errs.append(new SparkException(s"Couldn't find leader offsets for ${missing}"))
      Left(errs)
    }
  }

  // Consumer offset api
  // scalastyle:off
  // https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
  // scalastyle:on

  /** Requires Kafka >= 0.8.1.1 */
  def getConsumerOffsets(
                          groupId: String,
                          topicAndPartitions: Set[TopicAndPartition]
                        ): Either[Err, Map[TopicAndPartition, Long]] = {
    getConsumerOffsetMetadata(groupId, topicAndPartitions).right.map { r =>
      r.map { kv =>
        kv._1 -> kv._2.offset
      }
    }
  }

  /** Requires Kafka >= 0.8.1.1 */
  def getConsumerOffsetMetadata(
                                 groupId: String,
                                 topicAndPartitions: Set[TopicAndPartition]
                               ): Either[Err, Map[TopicAndPartition, OffsetMetadataAndError]] = {
    var result = Map[TopicAndPartition, OffsetMetadataAndError]()
    val req = OffsetFetchRequest(groupId, topicAndPartitions.toSeq)
    val errs = new Err
    withBrokers(Random.shuffle(config.seedBrokers), errs) { consumer =>
      val resp = consumer.fetchOffsets(req)
      val respMap = resp.requestInfo
      val needed = topicAndPartitions.diff(result.keySet)
      needed.foreach { tp: TopicAndPartition =>
        respMap.get(tp).foreach { ome: OffsetMetadataAndError =>
          if (ome.error == ErrorMapping.NoError) {
            result += tp -> ome
          } else {
            errs.append(ErrorMapping.exceptionFor(ome.error))
          }
        }
      }
      if (result.keys.size == topicAndPartitions.size) {
        return Right(result)
      }
    }
    val missing = topicAndPartitions.diff(result.keySet)
    errs.append(new SparkException(s"Couldn't find consumer offsets for ${missing}"))
    Left(errs)
  }

  /** Requires Kafka >= 0.8.1.1 */
  def setConsumerOffsets(
                          groupId: String,
                          offsets: Map[TopicAndPartition, Long]
                        ): Either[Err, Map[TopicAndPartition, Short]] = {
    setConsumerOffsetMetadata(groupId, offsets.map { kv =>
      kv._1 -> OffsetMetadataAndError(kv._2)
    })
  }

  /** Requires Kafka >= 0.8.1.1 */
  def setConsumerOffsetMetadata(
                                 groupId: String,
                                 metadata: Map[TopicAndPartition, OffsetMetadataAndError]
                               ): Either[Err, Map[TopicAndPartition, Short]] = {
    var result = Map[TopicAndPartition, Short]()
    val req = OffsetCommitRequest(groupId, metadata)
    val errs = new Err
    val topicAndPartitions = metadata.keySet
    withBrokers(Random.shuffle(config.seedBrokers), errs) { consumer =>
      val resp = consumer.commitOffsets(req)
      val respMap = resp.requestInfo
      val needed = topicAndPartitions.diff(result.keySet)
      needed.foreach { tp: TopicAndPartition =>
        respMap.get(tp).foreach { err: Short =>
          if (err == ErrorMapping.NoError) {
            result += tp -> err
          } else {
            errs.append(ErrorMapping.exceptionFor(err))
          }
        }
      }
      if (result.keys.size == topicAndPartitions.size) {
        return Right(result)
      }
    }
    val missing = topicAndPartitions.diff(result.keySet)
    errs.append(new SparkException(s"Couldn't set offsets for ${missing}"))
    Left(errs)
  }

  // Try a call against potentially multiple brokers, accumulating errors
  private def withBrokers(brokers: Iterable[(String, Int)], errs: Err)
                         (fn: SimpleConsumer => Any): Unit = {
    brokers.foreach { hp =>
      var consumer: SimpleConsumer = null
      try {
        consumer = connect(hp._1, hp._2)
        fn(consumer)
      } catch {
        case NonFatal(e) =>
          errs.append(e)
      } finally {
        if (consumer != null) {
          consumer.close()
        }
      }
    }
  }
}
private[streaming]
object KafkaCluster {
  type Err = ArrayBuffer[Throwable]

  case class LeaderOffset(host: String, port: Int, offset: Long)

  /**
    * High-level kafka consumers connect to ZK.  ConsumerConfig assumes this use case.
    * Simple consumers connect directly to brokers, but need many of the same configs.
    * This subclass won't warn about missing ZK params, or presence of broker params.
    */

  class SimpleConsumerConfig private(brokers: String, originalProps: Properties)
    extends ConsumerConfig(originalProps) {
    val seedBrokers: Array[(String, Int)] = brokers.split(",").map { hp =>
      val hpa = hp.split(":")
      if (hpa.size == 1) {
        throw new SparkException(s"Broker not the in correct format of <host>:<port> [$brokers]")
      }
      (hpa(0), hpa(1).toInt)
    }
  }

  private[streaming]
  object SimpleConsumerConfig {
    /**
      * Make a consumer config without requiring group.id or zookeeper.connect,
      * since communicating with brokers also needs common settings such as timeout
      */
    def apply(kafkaParams: Map[String, String]): SimpleConsumerConfig = {
      // These keys are from other pre-existing kafka configs for specifying brokers, accept either
      val brokers = kafkaParams.get("metadata.broker.list")
        .orElse(kafkaParams.get("bootstrap.servers"))
        .getOrElse(throw new SparkException(
          "Must specify metadata.broker.list or bootstrap.servers"))

      val props = new Properties()
      kafkaParams.foreach { case (key, value) =>
        // prevent warnings on parameters ConsumerConfig doesn't know about
        if (key != "metadata.broker.list" && key != "bootstrap.servers") {
          props.put(key, value)
        }
      }

      Seq("zookeeper.connect", "group.id").foreach { s =>
        if (!props.contains(s)) {
          props.setProperty(s, "")
        }
      }

      new SimpleConsumerConfig(brokers, props)
    }
  }
}