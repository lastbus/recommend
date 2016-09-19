package com.bl.bigdata.product

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job

/**
 * 销量排行：
 * 最近一天、七天，n 天，乘上一个权重
 * Created by MK33 on 2016/3/21.
 */
class HotSaleGoods extends Tool {

  override def run(args: Array[String]): Unit = {
    logger.info("品类热销开始计算........")
    val optionsMap = try {
      HotSaleConf.parse(args)
    } catch {
      case e: Throwable =>
        logger.error("command line parse error: " + e)
        HotSaleConf.printHelp
        return
    }
    optionsMap.foreach { case (k, v) => logger.info(k + " : " + v)}
    Message.addMessage("\n品类热销商品：\n")

    val input = optionsMap(HotSaleConf.input)
    val output = optionsMap(HotSaleConf.output)
    val prefix = optionsMap(HotSaleConf.goodsIdPrefix)
    val hbase = output.contains("hbase")
    val redis = output.contains("redis")

    val days = optionsMap(HotSaleConf.days).toInt
    val deadTimeOne = HotSaleGoods.getDateBeforeNow(days)
    // 权重系数
    val deadTimeOneIndex = 2
    val sc = SparkFactory.getSparkContext("品类热销商品")
    val accumulator = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)

    val sdf = new SimpleDateFormat(optionsMap(HotSaleConf.sdf))
    val date = new Date
    val start = sdf.format(new Date(date.getTime - 24000L * 3600 * 60))

    val sql = s"select u.goods_sid, u.goods_name, u.quanlity, u.event_date, u.category_sid, g.store_sid " +
              s" from recommendation.user_behavior_raw_data u inner join recommendation.goods_avaialbe_for_sale_channel g on g.sid = u.goods_sid  " +
              s" where u.dt >= $start and u.quanlity <> null and u.quanlity <> '' "

    val sqlName = optionsMap(HotSaleConf.sql)
    val rawRDD = DataBaseUtil.getData(input, sqlName, start).filter(a => a(0) != null && a(1) != null && a(2) != null && a(3) != null && a(4) != null)
    val result = rawRDD.filter(s => s != null && s(2).length >= 0 && !s(2).equalsIgnoreCase("NULL") )
                          .map { case Array(goodsID, goodsName, sale_num, sale_time, categoryID, storeId) =>
                                      (goodsID, goodsName, sale_num.toInt, sale_time, categoryID, storeId) }
                          .map { case (goodsID, goodsName, sale_num, sale_time, categoryID, storeId) =>
                                      ((goodsID, storeId), (goodsName, if(sale_time >= deadTimeOne) deadTimeOneIndex * sale_num else sale_num, categoryID))}
                          .reduceByKey((s1, s2) => (s1._1, s1._2 + s2._2, s1._3))
                          .map { case ((goodsID, storeId), (goodsName, sale_num, categoryID)) =>
                                      ((categoryID, storeId), Seq((goodsID, sale_num)))}
                          .reduceByKey((s1, s2) =>s1 ++ s2)
                          .map { case ((category, storeId), seq) =>
                            accumulator += 1
                            val key = if (storeId != null && storeId.length > 0 && !storeId.equalsIgnoreCase("NULL")) prefix + storeId + "_" + category else prefix + category
                            (key, seq.sortWith(_._2 > _._2).take(20).map(_._1).mkString("#"))
                          }
    result.cache()
    if(redis) {
      val redisType = if (output.contains("cluster")) RedisClient.cluster else RedisClient.standalone
      RedisClient.sparkKVToRedis(result, accumulator2, redisType)
      Message.addMessage(s"\t$prefix*: $accumulator\n")
      Message.addMessage(s"\t插入 redis $prefix*: $accumulator2\n")
    }

    if (hbase){
      // not use temporary
      val conf = HBaseConfiguration.create()
      conf.set(TableOutputFormat.OUTPUT_TABLE, "h1")
      val columnFamilyBytes = Bytes.toBytes("c1")
      val columnNameBytes = Bytes.toBytes("hot_sale")
      val c = sc.hadoopConfiguration
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
    logger.info("品类热销计算结束。")
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
object HotSaleConf  {

  val input = "input"
  val sql= "sql"
  val output = "output"

  val goodsIdPrefix = "goods id prefix"
  val days = "n-days"
  val sdf = "dateFormat"


  val buyGoodsSimCommand = new MyCommandLine("HotSale")
  buyGoodsSimCommand.addOption("i", input, true, "data.source", "hive")
  buyGoodsSimCommand.addOption("o", output, true, "output data to redis， hbase or HDFS", "redis-cluster")
  buyGoodsSimCommand.addOption(sql, sql, true, "user.behavior.raw.data", "hot.sale")
  buyGoodsSimCommand.addOption("pm", goodsIdPrefix, true, "goods id prefix", "rcmd_cate_hotsale_")
  buyGoodsSimCommand.addOption("n", days, true, "days before today", "30")
  buyGoodsSimCommand.addOption("sdf", sdf, true, "date format", "yyyyMMdd")

  def parse(args: Array[String]): Map[String, String] ={
    buyGoodsSimCommand.parser(args)
  }

  def printHelp = buyGoodsSimCommand.printHelper

}
