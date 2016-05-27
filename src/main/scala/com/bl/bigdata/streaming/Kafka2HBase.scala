package com.bl.bigdata.streaming

import java.io.{FileInputStream, InputStreamReader}
import java.net.ServerSocket
import java.util.Properties

import akka.actor.Actor
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable
import scala.util.Random

/**
 *
 *
 * Created by MK33 on 2016/5/24.
 */
object Kafka2HBase extends StreamingLogger {

  def main (args: Array[String]): Unit = {
    logger.info("=============      %s begin   ================.".format(this.getClass.getName.stripSuffix("$")))

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val props = new Properties()
    props.load(new InputStreamReader(new FileInputStream("streaming-kafka.properties")))

    val checkpointPath = props.getProperty("checkpointpath")
    val ssc = StreamingContext.getOrCreate(checkpointPath, () => createStreamingContext(props, checkpointPath))
    ssc.start()
    logger.info("Task started! ")
    ssc.awaitTermination()



  }

  def getHBaseJobConf = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "kafka-streaming-test")
    val job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[Put]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])
    job
  }

  val createStreamingContext = (props: Properties, checkpointPath: String) => {
    logger.info(s"create checkpoint file $checkpointPath")
    val kafkaParams = mutable.Map[String, String]()
    val keys = props.keySet().iterator()
    while (keys.hasNext) {
      val key = keys.next().toString
      kafkaParams(key) = props.getProperty(key)
    }
    val conf = new SparkConf().setAppName("kafka-streaming-hbase")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint(checkpointPath)
    val topics = props.getProperty("topic").split(",").toSet
    val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams.toMap, topics)

    val columnFamilyBytes = Bytes.toBytes(props.getProperty("hbase.column.family"))
    val columnNameBytes = Bytes.toBytes(props.getProperty("hbase.column.value"))

    val puts = message.map(s => {
      val random = new Random()
      val put = new Put(Bytes.toBytes(random.nextInt()))
      put.addColumn(columnFamilyBytes, columnNameBytes, Bytes.toBytes(s._2))
      //      (new ImmutableBytesWritable(Bytes.toBytes(s._2)), put)
      put
    })

    puts.foreachRDD { rdd =>
      val kafkaHBaseCount = kafkaAccumulator.getInstance(rdd.sparkContext)
      rdd.foreachPartition { partition =>
        val conn = HBaseConnectionPool.connection
//        val conn = ConnectionFactory.createConnection(conf)
        val table = conn.getTable(TableName.valueOf("kafka-streaming-test"))
        partition.foreach(put => {
          kafkaHBaseCount += 1
          table.put(put)
        })
        table.close()
      }
      logger.info(s"kafka-hbase-count:  ${kafkaHBaseCount.value}")
    }

    ssc
  }


}

object kafkaAccumulator {

  @volatile private var instance: Accumulator[Long] = null

  def getInstance(sc: SparkContext): Accumulator[Long] =
  {
    if (instance == null)
    {
      synchronized {
        if (instance == null) instance = sc.accumulator(0L, "kafka-hbase-count")
      }
    }
    instance
  }
}


class StopListenerActor(ssc: StreamingContext) extends Actor with StreamingLogger {
  override def receive: Receive = {
    case "start" =>
      val listen = new ServerSocket(9999)
      val socket = listen.accept()
      socket.getInputStream.read()
      while (true)
      {

      }
    case "stop" =>
      logger.info("stop streaming")
      ssc.stop(true, true)
      sys.exit(0)
    case e => logger.info("error message: %s".format(e))
  }
}