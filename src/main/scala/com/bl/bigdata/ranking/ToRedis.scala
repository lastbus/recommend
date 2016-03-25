package com.bl.bigdata.ranking

import java.util
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.{Jedis, Protocol, JedisPoolConfig, JedisPool}
import com.redislabs.provider.redis._
import com.bl.bigdata.util.RedisUtil._

/**
  * Created by MK33 on 2016/3/21.
  */
object ToRedis {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    sparkConf.set("redis.host", "10.201.128.216")
    sparkConf.set("redis.port", "6379")
    sparkConf.set("redis.timeout", "10000")
    val sc = new SparkContext(sparkConf)
    val jedisPool = getJedisPool
    toRedis2(sc, jedisPool)
    //    test(jedisPool)
        toRedis1(sc, jedisPool)
//        category(sc, jedisPool)
    jedisPool.destroy()
    sc.stop()
  }

  def toRedis2(sc: SparkContext, jedisPool: JedisPool): Unit = {
    val r = sc.textFile("D:\\2016-03-21\\goods_avaialbe_for_sale")
      .map(line => {
        val w = line.split("\t")
        (w(0), w(1), w(2), w(3), w(4), w(5), w(6), w(7), w(8), w(9))
      }).distinct.collect().foreach { case (sid, mdm_goods_sid, goods_sales_name, goods_type, pro_sid, brand_sid, cn_name,
    category_id, category_name, sale_price) => {
      val jedis = jedisPool.getResource
      val map = Map("sid" -> sid, "mdm_goods_sid" -> mdm_goods_sid, "goods_sales_name" -> goods_sales_name,
        "goods_type" -> goods_type, "pro_sid" -> pro_sid, "brand_sid" -> brand_sid, "cn_name" -> cn_name,
        "category_id" -> category_id, "category_name" -> category_name, "sale_price" -> sale_price)

//      if (!jedis.exists("rcmd_orig_" + sid)) {
//        println("rcmd_orig_" + sid)
        import com.bl.bigdata.util.Implicts.map2HashMap
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

  def test(jedisPool : JedisPool) = {
    val jedis = jedisPool.getResource
    val v = jedis.get("rcmd_cate_194453")
    println(v)
  }

  def toRedis1(sc: SparkContext, jedisPool: JedisPool): Unit = {

    val readRDD = sc.textFile("D:\\2016-03-21\\goods_avaialbe_for_sale").map(line => {
      val w = line.split("\t")
      // goodsID, categoryID
      (w(0), w(7))
    }).distinct

    val count1 = sc.accumulator(0, "goodsID-categoryID")
    val count2 = sc.accumulator(0, "categoryID-goodsIDs")

    val goodsIDToCategoryIDRDD = readRDD map { case (goodsID, categoryID) => ("rcmd_cate_" + goodsID, categoryID)}
    sc.toRedisKV(goodsIDToCategoryIDRDD)
//println(count1)
    val categoryIDToGoodsID = readRDD.map(s => (s._2, Seq(s._1))).reduceByKey(_ ++ _)
        .map { case (categoryID, goodsID) => ("rcmd_cate_goods_" + categoryID, goodsID.mkString("#"))}

    sc.toRedisKV(categoryIDToGoodsID)

    println(
      s"""count1: $count1

          |count2: $count2
       """.
        stripMargin)
  }
}
