package com.bl.bigdata.product

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD

/**
 * 销量排行：
 * 最近一天、七天，n 天，乘上一个权重
 * Created by MK33 on 2016/3/21.
 */
class HotSaleGoods extends Tool {

  override def run(args: Array[String]): Unit = {
    logger.info("品类热销开始计算........")
    Message.addMessage("\n品类热销商品：\n")
    val output = ConfigurationBL.get("recmd.output")
    val prefix = ConfigurationBL.get("hot.sale")
    val hbase = output.contains("hbase")
    val redis = output.contains("redis")

    val deadTimeOne = HotSaleGoods.getDateBeforeNow(30)
    val deadTimeOneIndex = 2
    val sc = SparkFactory.getSparkContext("品类热销商品")
    val accumulator = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)

    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date = new Date
    val start = sdf.format(new Date(date.getTime - 24000L * 3600 * 60))
    val sql = s"select goods_sid, goods_name, quanlity, event_date, category_sid " +
              s" from recommendation.user_behavior_raw_data where dt >= $start"

    val result = ReadData.readHive(sc, sql)
                          .map { case Array(goodsID, goodsName, sale_num, sale_time, categoryID) =>
                                      (goodsID, goodsName, sale_num.toInt, sale_time, categoryID) }
                          .map { case (goodsID, goodsName, sale_num, sale_time, categoryID) =>
                                      (goodsID, (goodsName, if(sale_time >= deadTimeOne) deadTimeOneIndex * sale_num else sale_num, categoryID))}
                          .reduceByKey((s1, s2) => (s1._1, s1._2 + s2._2, s1._3))
                          .map { case (goodsID, (goodsName, sale_num, categoryID)) =>
                                      (categoryID, Seq((goodsID, sale_num)))}
                          .reduceByKey((s1, s2) =>s1 ++ s2)
                          .map { case (category, seq) => {accumulator += 1; (prefix + category, seq.sortWith(_._2 > _._2).take(20).map(_._1).mkString("#")) }}
    result.cache()
    if(redis) {
//      sc.toRedisKV(result)
      saveToRedis(result, accumulator2)
      Message.addMessage(s"\t$prefix*: $accumulator\n")
      Message.addMessage(s"\t插入 redis $prefix*: $accumulator2\n")
    }

    if (hbase){
//      val conf = HBaseConfiguration.create()
//      conf.set(TableOutputFormat.OUTPUT_TABLE, "h1")
//      val columnFamilyBytes = Bytes.toBytes("c1")
//      val columnNameBytes = Bytes.toBytes("hot_sale")
//      val c = sc.hadoopConfiguration
//      c.set(TableOutputFormat.OUTPUT_TABLE, "h1")
//      val job = Job.getInstance(c)
//      job.setOutputFormatClass(classOf[TableOutputFormat[Put]])
//      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
//      job.setOutputValueClass(classOf[Result])
//      result.map{ case (k, v) => {
//        val put = new Put(Bytes.toBytes(k))
//        (new ImmutableBytesWritable(k.getBytes()), put.addColumn(columnFamilyBytes, columnNameBytes, Bytes.toBytes(v)))
//      }}.saveAsNewAPIHadoopDataset(job.getConfiguration)

    }
    result.unpersist()
    logger.info("品类热销计算结束。")
  }

  def saveToRedis(rdd: RDD[(String, String)], accumulator: Accumulator[Int]): Unit = {
    rdd.foreachPartition(partition => {
      try {
        val jedis = RedisClient.pool.getResource
        partition.foreach(s => {
          jedis.set(s._1, s._2)
          accumulator += 1
        })
        jedis.close()
      } catch {
        case e: Exception => Message.addMessage(e.getMessage)
      }
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
