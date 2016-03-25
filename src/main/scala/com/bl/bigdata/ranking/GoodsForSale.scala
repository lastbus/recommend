package com.bl.bigdata.ranking

import com.bl.bigdata.util.RedisUtil._
import com.bl.bigdata.util.{ConfigurationBL, Tool, ToolRunner}
import com.redislabs.provider.redis._
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.JedisPool

/**
  * 计算商品的属性，保存成redis中hash值。
  * Created by MK33 on 2016/3/21.
  */
class GoodsForSale extends Tool {
  private val logger = LogManager.getLogger(this.getClass.getName)

  def toRedis2(rdd: RDD[String], jedisPool: JedisPool): Unit = {
    val delimiter = ConfigurationBL.get("goods.for.sale.delimiter", "\t")
    val r = rdd
      .map(line => {
        val w = line.split(delimiter)
        (w(0), w(1), w(2), w(3), w(4), w(5), w(6), w(7), w(8), w(9), w(10), w(11))
      }).distinct.collect().foreach { case (sid, mdm_goods_sid, goods_sales_name, goods_type, pro_sid, brand_sid, cn_name,
    category_id, category_name, sale_price, pic_id, url) => {
      val jedis = jedisPool.getResource
      val map = Map("sid" -> sid, "mdm_goods_sid" -> mdm_goods_sid, "goods_sales_name" -> goods_sales_name,
        "goods_type" -> goods_type, "pro_sid" -> pro_sid, "brand_sid" -> brand_sid, "cn_name" -> cn_name,
        "category_id" -> category_id, "category_name" -> category_name, "sale_price" -> sale_price,
        "pic_sid" -> pic_id, "url" -> url)

//      if (!jedis.exists("rcmd_orig_" + sid)) {
//        println("rcmd_orig_" + sid)
        import com.bl.bigdata.util.implicts.map2HashMap
        jedis.hmset("rcmd_orig_" + sid, map)
//      }
      jedis.close()
    }}
  }

  /**
    * 分类
    *
    * @param sc
    * @param jedisPool
    */
  def category(sc: SparkContext, jedisPool: JedisPool): Unit = {
    val readRDD = sc.textFile("D:\\2\\dim_category").map(line => {
      val w = line.split("\t")
      val size = w.length
      // categoryID, 一级目录, 二级目录, 三级目录.....
      (w(0), w(size - 4), w(size - 2))})
      .map{ case (goodsID, parents, sub) => (if(goodsID == sub) parents else sub, Seq((goodsID)))}
      .distinct().reduceByKey(_ ++ _).collect().foreach { case (parent, children) =>
        val jedis = jedisPool.getResource
        for (child <- children) jedis.set("rcmd_parent_category_" + child, parent)
        jedis.set("rcmd_subcategory_" + parent, children.mkString("#"))
        jedis.close()
    }

  }

  def toRedis1(rdd: RDD[String], jedisPool: JedisPool): Unit = {
    val readRDD = rdd.map(line => {
      val w = line.split("\t")
      // goodsID, categoryID
      (w(0), w(7))
    }).distinct
    val sc = rdd.sparkContext
    val goodsIDToCategoryIDRDD = readRDD map { case (goodsID, categoryID) => ("rcmd_cate_" + goodsID, categoryID)}
    sc.toRedisKV(goodsIDToCategoryIDRDD)
    val categoryIDToGoodsID = readRDD.map(s => (s._2, Seq(s._1))).reduceByKey(_ ++ _)
        .map { case (categoryID, goodsID) => ("rcmd_cate_goods_" + categoryID, goodsID.mkString("#"))}
    sc.toRedisKV(categoryIDToGoodsID)
  }

  override def run(args: Array[String]): Unit = {

    ConfigurationBL.parseConfFile("recmd-conf.xml")
    val inputPath = ConfigurationBL.get("goods.for.sale.input.path")
    val outputPath = ConfigurationBL.get("goods.for.sale.output.path")
    val local = outputPath.contains("local")
    val redis = outputPath.contains("redis")

    val sparkConf = new SparkConf().setAppName(ConfigurationBL.get("goods.for.sale.app.name", this.getClass.getName))
    if (local) sparkConf.setMaster("local[*]")
    if (redis) {
      for ((k, v) <- ConfigurationBL.getAll if k.startsWith("redis."))
        sparkConf.set(k, v)
    }
    val sc = new SparkContext(sparkConf)
    val rawRDD = sc.textFile(inputPath)
    val jedisPool = getJedisPool
    toRedis1(rawRDD, jedisPool)
    toRedis2(rawRDD, jedisPool)
    jedisPool.destroy()
    sc.stop()
  }
}

object GoodsForSale {

  def main(args: Array[String]) {
    (new GoodsForSale with ToolRunner).run(args)
  }
}
