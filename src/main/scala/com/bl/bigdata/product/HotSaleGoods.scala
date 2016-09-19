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
    val optionsMap = HotSaleConf.parse(args)

    logger.info("品类热销开始计算........")
    // 输出参数
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

    val sql = s"select goods_sid, goods_name, quanlity, event_date, category_sid " +
              s" from recommendation.user_behavior_raw_data where dt >= $start and quanlity <> null and quanlity <> '' "

    val sqlName = optionsMap(HotSaleConf.sql)
    val rawRDD = DataBaseUtil.getData(input, sqlName, start).filter(_.contains(null))
    val result = rawRDD.filter(s => s != null && s(2).length >= 0 && !s.contains(null) && !s(2).equalsIgnoreCase("NULL") )
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
      val redisType = if (output.contains("cluster")) "cluster" else "standalone"
//      sc.toRedisKV(result)
//      saveToRedis(result, accumulator2, redisType)
      RedisClient.sparkKVToRedis(result, accumulator2, redisType)
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

  def saveToRedis(rdd: RDD[(String, String)], accumulator: Accumulator[Int], redisType: String): Unit = {
    rdd.foreachPartition(partition => {
      try {
        redisType match {
          case "cluster" =>
            val jedis = RedisClient.jedisCluster
            partition.foreach { s =>
              jedis.set(s._1, s._2)
              accumulator += 1
            }
            jedis.close()
          case "standalone" =>
            val jedis = RedisClient.pool.getResource
            partition.foreach { s =>
              jedis.set(s._1, s._2)
              accumulator += 1
            }
            jedis.close()
          case _ => logger.error(s"wrong redis type $redisType ")
        }
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
  buyGoodsSimCommand.addOption("pm", goodsIdPrefix, true, "goods id prefix", "rcmd_cookieid_view_")
  buyGoodsSimCommand.addOption("n", days, true, "days before today", "30")
  buyGoodsSimCommand.addOption("sdf", sdf, true, "date format", "yyyyMMdd")

  def parse(args: Array[String]): Map[String, String] ={
    buyGoodsSimCommand.parser(args)
  }

}
