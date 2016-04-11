package com.bl.bigdata.ranking

import java.util

import com.bl.bigdata.mail.{Message, MailServer}
import com.bl.bigdata.util._
import com.redislabs.provider.redis._
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
  * 计算商品的属性，保存成redis中hash值。
  * Created by MK33 on 2016/3/21.
  */
class GoodsForSale extends Tool {

  private val logger = LogManager.getLogger(this.getClass.getName)
  private val message = new StringBuilder

  override def run(args: Array[String]): Unit = {
    message.append("goods for sale:\n")
    logger.info("execute goods for sale.")
    val inputPath = ConfigurationBL.get("goods.for.sale.input.path")
    val outputPath = ConfigurationBL.get("goods.for.sale.output.path")
    val local = outputPath.contains("local")
    val redis = outputPath.contains("redis")

    val sparkConf = new SparkConf()
      .setAppName(ConfigurationBL.get("goods.for.sale.app.name", this.getClass.getName))
    if (local) sparkConf.setMaster("local[*]")
    if (redis) {
      for ((k, v) <- ConfigurationBL.getAll if k.startsWith("redis."))
        sparkConf.set(k, v)
    }
    val sc = SparkFactory.getSparkContext()
    val hiveContext = new HiveContext(sc)
//    val rawRDD = sc.textFile(inputPath)
    toRedis1(hiveContext)
    toRedis2(hiveContext)
    category(hiveContext)

    Message.message.append(message)

    sc.stop()
  }

  /**
    *
    * @param hiveContext
    */
  def toRedis1(hiveContext: HiveContext): Unit = {
    val sql = "select sid, category_id from recommendation.goods_avaialbe_for_sale"

//    val readRDD = rdd.map(line => {
//      val w = line.split("\t")
//      // goodsID, categoryID
//      (w(0), w(7))
//    }).distinct
    val readRDD = hiveContext.sql(sql).rdd.map(row => (row.getString(0), row.getLong(1).toString)).distinct()
    val sc = hiveContext.sparkContext
    val count1Accumulator = sc.accumulator(0)
    val count2Accumulator = sc.accumulator(0)
    val goodsIDToCategoryIDRDD = readRDD map { case (goodsID, categoryID) => {
      count1Accumulator += 1
      ("rcmd_cate_" + goodsID, categoryID)
    }}
//    sc.toRedisKV(goodsIDToCategoryIDRDD)
    saveToRedis(goodsIDToCategoryIDRDD, count1Accumulator)
    message.append(s"插入 rcmd_cate_* : $count1Accumulator\n")

    val categoryIDToGoodsID = readRDD.map(s => (s._2, Seq(s._1))).reduceByKey(_ ++ _)
      .map { case (categoryID, goodsID) => {
        count2Accumulator += 1
        ("rcmd_cate_goods_" + categoryID, goodsID.mkString("#"))
      }}
//    sc.toRedisKV(categoryIDToGoodsID)
    saveToRedis(categoryIDToGoodsID, count2Accumulator)
    message.append(s"插入 rcmd_cate_goods_*: $count2Accumulator\n")

  }

  /**
    * 统计商品属性，保存在 redis 中。
 *
    * @param hiveContext
    */
  def toRedis2(hiveContext: HiveContext): Unit = {
    import com.bl.bigdata.util.MyRedisContext._
    val delimiter = ConfigurationBL.get("goods.for.sale.delimiter", "\t")
    val sql = "select * from recommendation.goods_avaialbe_for_sale"
    val sc = hiveContext.sparkContext
    val accumulator = sc.accumulator(0)
//    val r = hiveContext
//      .map(line => {
//        val w = line.split(delimiter)
//        (w(0), w(1), w(2), w(3), w(4), w(5), w(6), w(7), w(8), w(9), w(10), w(11))
//      })
    val r = hiveContext.sql(sql).rdd.
      map(row => (row.getString(0), row.getString(1), row.getString(2),
                  row.getDouble(3).toString, row.getString(4), row.getString(5),
                  row.getString(6), row.getLong(7).toString, row.getString(8),
                  row.getDouble(9).toString, row.getString(10), row.getString(11)))
      .distinct.map{ case (sid, mdm_goods_sid, goods_sales_name, goods_type, pro_sid, brand_sid, cn_name,
    category_id, category_name, sale_price, pic_id, url) => {
      val map = Map("sid" -> sid, "mdm_goods_sid" -> mdm_goods_sid, "goods_sales_name" -> goods_sales_name,
        "goods_type" -> goods_type, "pro_sid" -> pro_sid, "brand_sid" -> brand_sid, "cn_name" -> cn_name,
        "category_id" -> category_id, "category_name" -> category_name, "sale_price" -> sale_price,
        "pic_sid" -> pic_id, "url" -> url)
      val m = new util.HashMap[String, String]
      for ((k, v) <- map) m.put(k, v)
      ("rcmd_orig_" + sid, m)
    }}
//    sc.hashKVRDD2Redis(r)
    saveToRedisHash(r, accumulator)
    message.append(s"插入 rcmd_orig_*: $accumulator\n")
  }

  def saveToRedisHash(rdd: RDD[(String, java.util.HashMap[String, String])], accumulator: Accumulator[Int]): Unit ={
    rdd.foreachPartition(partition => {
      val jedis = RedisClient.pool.getResource
      partition.foreach(data => {
        jedis.hmset(data._1, data._2)
        accumulator += 1
      })
      jedis.close()

    })
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


  /**
    * 根据商品 id 得到它上一级品类ID；
    * 根据品类ID，得到所属的商品ID列表
 *
    * @param hiveContext
    */
  def category(hiveContext: HiveContext): Unit = {
    val inputPath = ConfigurationBL.get("goods.for.sale.category.input.path")
    val sc = hiveContext.sparkContext
    val accumulator1 = sc.accumulator(0)
    val accumulator2 = sc.accumulator(0)
    val sql = "select category_id, level2_id, level3_id from recommendation.dim_category"
//    val readRDD = hiveContext.textFile(inputPath).map(line => {
//      val w = line.split("\t")
//      val size = w.length
//      // categoryID, 一级目录, 二级目录, 三级目录.....
//      (w(0), w(size - 4), w(size - 2))})
    val readRDD = hiveContext.sql(sql).rdd.map(row =>
                  if (!row.anyNull) (row.getLong(0).toString, row.getLong(1).toString, row.getLong(2).toString) else null).filter(_ != null)
      .map{ case (goodsID, parents, sub) => (if(goodsID == sub) parents else sub, Seq((goodsID)))}
      .distinct().reduceByKey(_ ++ _).map { case (parent, children) =>
      val size = children.length
      accumulator1 += size
      val array = new Array[(String, String)](size + 1)
      for (i <- 0 until size)
        array(i) = ("rcmd_parent_category_" + children(i), parent)
      array(size) = ("rcmd_subcategory_" + parent, children.mkString("#"))
      array
    }.flatMap(s => s)
//    sc.toRedisKV(readRDD)
    saveToRedis(readRDD, accumulator2)
//    message.append(s"插入 rcmd_parent_category_*: $accumulator1\n")
    message.append(s"插入 rcmd_parent_category_*: $accumulator2\n")
    message.append(s"插入 rcmd_subcategory_*: $accumulator2\n")
  }

}

object GoodsForSale {

  def main(args: Array[String]) {
    execute(args)
  }

  def execute(args: Array[String]): Unit ={
    val test = new GoodsForSale with ToolRunner
    test.run(args)
//    MailServer.send(test.message.toString)
//    test.message.clear()
  }
}
