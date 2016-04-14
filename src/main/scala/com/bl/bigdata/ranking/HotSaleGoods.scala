package com.bl.bigdata.ranking

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Accumulator, SparkConf}

/**
 * 销量排行：
 * 最近一天、七天，n 天，乘上一个权重
 * Created by MK33 on 2016/3/21.
 */
class HotSaleGoods extends Tool {

  private val logger = LogManager.getLogger(this.getClass)

  override def run(args: Array[String]): Unit = {
    Message.addMessage("\n品类热销商品：\n")
    val output = ConfigurationBL.get("recmd.output")
    val hbase = output.contains("hbase")
    val redis = output.contains("redis")
    val local = output.contains("local")

    val deadTimeOne = HotSaleGoods.getDateBeforeNow(30)
    val deadTimeOneIndex = 2
    val sparkConf = new SparkConf().setAppName("品类热销商品")
    if (local) sparkConf.setMaster("local[*]")

    if (redis)
      for ((key, value) <- ConfigurationBL.getAll.filter(_._1.startsWith("redis.")))
        sparkConf.set(key, value)
    val sc = SparkFactory.getSparkContext()

    val accumulator = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)


    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = new Date
    val start = sdf.format(new Date(date.getTime - 24000L * 3600 * 60))
    val sql = s"select goods_sid, goods_name, quanlity, event_date, category_sid from recommendation.user_behavior_raw_data where dt >= $start"

    val hiveContext = new HiveContext(sc)
//    val result = sc.textFile(input).map(line => {
//      val w = line.split("\t")
//      (w(3), w(4), w(6).toDouble, w(7), w(9), w(10))
//    })
//      .filter(item => HotSaleGoods.filterDate(item, deadTimeTwo))
    val result = hiveContext.sql(sql).rdd.
      map(row => if (!row.anyNull) (row.getString(0), row.getString(1), row.getInt(2),
                                    row.getString(3), row.getString(4)) else null).filter(_ != null)
      .map { case (goodsID, goodsName, sale_num, sale_time, categoryID) =>
        (goodsID, (goodsName, if(sale_time >= deadTimeOne) deadTimeOneIndex * sale_num else sale_num, categoryID))
      }.reduceByKey((s1, s2) => (s1._1, s1._2 + s2._2, s1._3))
      .map { case (goodsID, (goodsName, sale_num, categoryID)) =>
        (categoryID, Seq((goodsID, sale_num)))
      }
      .reduceByKey((s1, s2) =>s1 ++ s2)
      .map { case (category, seq) => {accumulator += 1; ("rcmd_cate_hotsale_" + category, seq.sortWith(_._2 > _._2).take(20).map(_._1).mkString("#"))} }
    result.cache()
    if(redis) {
      logger.info("start to write 热销 to redis.")
//      sc.toRedisKV(result)
      saveToRedis(result, accumulator2)
      logger.info("write finished.")
      Message.addMessage(s"\t品类热销商品: $accumulator\n")
      Message.addMessage(s"\t插入redis 品类热销商品: $accumulator2\n")
    }

    if (hbase){
      val conf = HBaseConfiguration.create()
      conf.set(TableOutputFormat.OUTPUT_TABLE, "h1")
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("hbase.zookeeper.quorum", "10.201.129.81")
      conf.set("hbase.master", "10.201.129.78:60000")

      val columnFamilyBytes = Bytes.toBytes("c1")
      val columnNameBytes = Bytes.toBytes("hot_sale")

      val c = sc.hadoopConfiguration
      c.addResource("hbase-site.xml")
      c.set(TableOutputFormat.OUTPUT_TABLE, "h1")
      val job = Job.getInstance(c)
      job.setOutputFormatClass(classOf[TableOutputFormat[Put]])
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])
      result.map{ case (k, v) => {
        val put = new Put(Bytes.toBytes(k))
        (new ImmutableBytesWritable(k.getBytes()), put.addColumn(columnFamilyBytes, columnNameBytes, Bytes.toBytes(v)))
      }}.saveAsNewAPIHadoopDataset(job.getConfiguration)

    }
    result.unpersist()
  }

  def saveToRedis(rdd: RDD[(String, String)], accumulator: Accumulator[Int]): Unit = {
    rdd.foreachPartition(partition => {
      val jedis = RedisClient.pool.getResource
      partition.foreach(s => {
        accumulator += 1
        jedis.set(s._1, s._2)
      })
      jedis.close()
    })
  }
}

object HotSaleGoods {

  def main(args: Array[String]): Unit = {
    execute(args)
  }

  def execute(args: Array[String]): Unit = {
    val hotSale = new HotSaleGoods with ToolRunner
    hotSale.run(args)
  }

  /**
    * 计算今天前 n 天的日期
 *
    * @param n 前 n 天
    * @return n 天前的日期
    */
  def getDateBeforeNow(n: Int): String = {
    val now = new Date
    val beforeMill = now.getTime - 24L * 60 * 60 * 1000 * n
    val before = new Date(beforeMill)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    sdf.format(before)
  }

  def filterDate(item: (String, String, Double, String, String, String), deadTime: String): Boolean = {
    item._4 >= deadTime
  }
}
