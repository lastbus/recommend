package com.bl.bigdata.ranking

import java.util

import com.bl.bigdata.mail.Message
import com.bl.bigdata.util._
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

/**
  * 计算商品的属性，保存成redis中hash值。
  * Created by MK33 on 2016/3/21.
  */
class GoodsForSale extends Tool {

  override def run(args: Array[String]): Unit = {
    Message.addMessage("\ngoods for sale:\n")
    logger.info("execute goods for sale.")
    val outputPath = ConfigurationBL.get("recmd.output")
    val sc = SparkFactory.getSparkContext("goods.for.sale")
    val hiveContext = new HiveContext(sc)
    toRedis1(hiveContext)
    toRedis2(hiveContext)
    category(hiveContext)
  }

  /** 商品类别下面有哪些商品，某个商品属于那个类别 */
  def toRedis1(hiveContext: HiveContext) = {

    val sql = "select sid, category_id from recommendation.goods_avaialbe_for_sale_channel where channel_sid = '3'"

    val readRDD = hiveContext.sql(sql).rdd.map(row => (row.getString(0), row.getLong(1).toString)).distinct()
    val sc = hiveContext.sparkContext
    val count1Accumulator = sc.accumulator(0)
    val count2Accumulator = sc.accumulator(0)
    val goodsIDToCategoryIDRDD = readRDD map { case (goodsID, categoryID) =>
      count1Accumulator += 1
      ("rcmd_cate_" + goodsID, categoryID)
    }
//    sc.toRedisKV(goodsIDToCategoryIDRDD)
    saveToRedis(goodsIDToCategoryIDRDD, count1Accumulator)
    Message.addMessage(s"\t插入 rcmd_cate_* : $count1Accumulator\n")

    val categoryIDToGoodsID = readRDD.map(s => (s._2, Seq(s._1))).reduceByKey(_ ++ _)
      .map { case (categoryID, goodsID) =>
        count2Accumulator += 1
        ("rcmd_cate_goods_" + categoryID, goodsID.mkString("#"))
      }
//    sc.toRedisKV(categoryIDToGoodsID)
    saveToRedis(categoryIDToGoodsID, count2Accumulator)
    Message.addMessage(s"\t插入 rcmd_cate_goods_*: $count2Accumulator\n")

  }

  /**
    * 统计商品属性，保存在 redis 中。
    * @param hiveContext 读取 hive 表
    */
  def toRedis2(hiveContext: HiveContext) = {
    val sql = "select sid, mdm_goods_sid, goods_sales_name, goods_type, pro_sid, brand_sid, " +
                      "cn_name, category_id, category_name, sale_price, pic_sid, url, channel_sid  " +
              "from recommendation.goods_avaialbe_for_sale_channel where channel_sid == '3' or channel_sid = '1' "
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
    row.getDouble(9).toString, row.getString(10), row.getString(11),
    row.getString(12)))
  .distinct().map{ case (sid, mdm_goods_sid, goods_sales_name, goods_type, pro_sid, brand_sid, cn_name,
    category_id, category_name, sale_price, pic_id, url, channel) =>
  val map = Map("sid" -> sid, "mdm_goods_sid" -> mdm_goods_sid, "goods_sales_name" -> goods_sales_name,
    "goods_type" -> goods_type, "pro_sid" -> pro_sid, "brand_sid" -> brand_sid, "cn_name" -> cn_name,
    "category_id" -> category_id, "category_name" -> category_name, "sale_price" -> sale_price,
    "pic_sid" -> pic_id, "url" -> url)
  val m = new util.HashMap[String, String]
  for ((k, v) <- map) m.put(k, v)
  val terminal = if ( channel == "3") "pc_" else "app_"
  ("rcmd_orig_" + terminal + sid, m)
}
//    sc.hashKVRDD2Redis(r)
    saveToRedisHash(r, accumulator)
    Message.addMessage(s"\t插入 rcmd_orig_pc/app_*: $accumulator\n")
  }

  def saveToRedisHash(rdd: RDD[(String, java.util.HashMap[String, String])], accumulator: Accumulator[Int]) ={
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
    * @param hiveContext 读取 hive 表
    */
  def category(hiveContext: HiveContext): Unit = {
//    val inputPath = ConfigurationBL.get("goods.for.sale.category.input.path")
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
      .map{ case (goodsID, parents, sub) => (if(goodsID == sub) parents else sub, Seq(goodsID))}
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
    Message.addMessage(s"\t插入 rcmd_parent_category_*: $accumulator2\n")
    Message.addMessage(s"\t插入 rcmd_subcategory_*: $accumulator2\n")
  }

}

object GoodsForSale {

  def main(args: Array[String]) {
    execute(args)
  }

  def execute(args: Array[String]): Unit ={
    val test = new GoodsForSale with ToolRunner
    test.run(args)
  }
}
