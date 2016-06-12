package com.bl.bigdata.mahout

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob

/**
 *
 * Created by MK33 on 2016/6/3.
 */
class HiveDataRaking extends Tool
{

  override def run(args: Array[String]): Unit =
  {
    logger.info(" ====== mahout recommendation =====")
    val tempOutput = ConfigurationBL.get("mahout.tmp.spark.output")
    val output = ConfigurationBL.get("mahout.tmp.hadoop.output")
    val dateCount = ConfigurationBL.get("mahout.day.count")
//    val Array(tempOutput, output, dateCount) = args
    val start = getStartTime(dateCount)
    val sql = s"select member_id, behavior_type, goods_sid, dt " +
      s"from recommendation.user_behavior_raw_data " +
      s"where dt >= $start and isnotnull(member_id)"
    val sc = SparkFactory.getSparkContext("user-item-rating")
    val rawRDD = ReadData.readHive(sc, sql)
    // 过滤得到浏览、购买、加入购物车等等操作
    val nowMills = new Date().getTime
    val ratingRDD = rawRDD.map { case Array(cookie_id, behavior_type, goods_sid, dt) =>
                                    (cookie_id, behavior_type, goods_sid, dt) }
                          .filter (s => s._2 == "1000" || s._2 == "2000" ||s._2 == "3000" || s._2 == "4000" )
                          .map { item => (item._1, item._3, getRating(item._2, item._4, nowMills)) }
    val users = ratingRDD.map(_._1).zipWithIndex()
    val tmp = "/tmp/userIndex"
    val fs = FileSystem.get(new Configuration)
    if (fs.exists(new Path(tmp))) fs.delete(new Path(tmp), true)
    users.map(s => s._1 + "," + s._2).saveAsTextFile(tmp)

    val training = ratingRDD.map(s => (s._1, (s._2, s._3))).join(users)
                            .map { case (user, ((prod, rating), userIndex)) => (userIndex, prod, rating)}
    if (fs.exists(new Path(tempOutput))) fs.delete(new Path(tempOutput), true)
    training.map(s => s"${s._1},${s._2},${s._3}").saveAsTextFile(tempOutput)
//    sc.stop()

    //  mahout 临时工作目录
    val tempPath = "/tmp/mahout"
    if (fs.exists(new Path(tempPath))) fs.delete(new Path(tempPath), true)
    if (fs.exists(new Path(output))) fs.delete(new Path(output), true)

    val job = new RecommenderJob()
    val sb = new StringBuilder()
    sb.append(" -s SIMILARITY_LOGLIKELIHOOD ")
    sb.append(" --numRecommendations 50  ")
    sb.append(s" --input $tempOutput ")
    sb.append(s" --output $output ")
    sb.append(s"--tempDir $tempPath ")
    val args2 = sb.toString().split(" ").filter(_.length > 1)
    job.run(args2)

    val sc2 = SparkFactory.getSparkContext("user-item-rating")
    val users1 = sc2.textFile(tmp).map (s => {val a = s.split(","); (a(1), a(0))})
//    val items2 = sc.textFile("/user/spark/items").map(s => {val a = s.split(","); (a(0), a(1))})
    val hadoopResult = sc2.textFile(output).map { s => val ab = s.split("\t"); (ab(0), ab(1)) }
    val itemBasedResult = hadoopResult.join(users1).map { case (userIndex, (prefer, userId)) => (userId, prefer) }.distinct()

    val table = ConfigurationBL.get("mahout.table")
    val columnFamily = ConfigurationBL.get("mahout.table.column.family")
    val columnFamilyBytes = Bytes.toBytes(columnFamily)
    val columnBytes = Bytes.toBytes("item-based")
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val mrJob = Job.getInstance(hBaseConf)
    mrJob.setOutputFormatClass(classOf[TableOutputFormat[Put]])
    mrJob.setOutputKeyClass(classOf[ImmutableBytesWritable])
    mrJob.setOutputValueClass(classOf[Put])
    val userAccumulator = sc2.accumulator(0)
    itemBasedResult.map { item =>
      val itemList = item._2.substring(1, item._2.length - 2)
      val put = new Put(Bytes.toBytes(item._1))
      put.addColumn(columnFamilyBytes, columnBytes, Bytes.toBytes(itemList))
      userAccumulator += 1
      (new ImmutableBytesWritable(Bytes.toBytes("")), put)
    }.saveAsNewAPIHadoopDataset(mrJob.getConfiguration)

    Message addMessage s"user number: $userAccumulator"

//    sc2.stop()

  }

  def getStartTime(d: String) =
  {
    val limit = d.toInt
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = new Date
    sdf.format(new Date(date.getTime - 24000L * 3600 * limit))
  }

  def getRating(behavior: String, date: String, now: Long) =
  {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val delt = (now - sdf.parse(date).getTime) / (24000L * 3600)
    val pow = behavior match {
      case "1000" => 1 case "2000" => 2 case "3000" => -1.5 case "4000" => 3
    }
    pow * Math.pow(0.95, delt)
  }

}

object HiveDataRaking
{

  def main(args: Array[String])
  {
    ConfigurationBL.init()
    val tool = new HiveDataRaking with Timer
    try {
      tool.run(args)
    } catch {
      case e: Throwable =>
        println(e getMessage)
        Message addMessage (e getMessage)
    }
    SparkFactory.destroyResource()
  }
}
