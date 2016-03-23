package com.bl.bigdata.ranking

import org.apache.spark.{SparkConf, SparkContext}
import com.redislabs.provider.redis._

/**
  * Created by MK33 on 2016/3/22.
  */
object SparkRedis {

  def main(args: Array[String]): Unit ={
    val sparkConf = new SparkConf().setMaster("local")
      .setAppName("myApp")
      // initial redis host - can be any node in cluster mode
      .set("redis.host", "10.201.128.216")
      // initial redis port
      .set("redis.port", "6379")
      // optional redis AUTH password
//      .set("redis.auth", "")
      .set("redis.timeout", "10000")

    val sc = new SparkContext(sparkConf)

    val r = sc.textFile("D:\\2016-03-21\\goods_avaialbe_for_sale")
      .map(line =>{
        val w = line.split("\t")
        (w(0), w(1), w(2), w(3), w(4), w(5), w(6), w(7), w(8), w(9))
      }).map{ case (sid, mdm_goods_sid, goods_sales_name, goods_type, pro_sid, brand_sid, cn_name,
    category_id, category_name, sale_price) => {
      val map = Map(sid + "sid" -> sid, sid + "mdm_goods_sid" -> mdm_goods_sid, sid + "goods_sales_name" -> goods_sales_name,
        sid + "goods_type" -> goods_type, sid + "pro_sid" -> pro_sid, sid + "brand_sid" -> brand_sid, sid + "cn_name" -> cn_name,
        sid + "category_id" -> category_id, sid + "category_name" -> category_name, sid + "sale_price" -> sale_price)
      map
    }}.flatMap(m => m.seq)

    sc.toRedisHASH(r, "rcmd_orig")

//    val t = sc.parallelize(List(("age", "27"), ("name", "make")))
//    sc.toRedisKV(t)
//    val r = sc.fromRedisKV("rcmd_cate_hotsale_*").count()
//    println(r)


  }

}
