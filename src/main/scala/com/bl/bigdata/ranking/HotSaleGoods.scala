package com.bl.bigdata.ranking

import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.{JedisPoolConfig, JedisPool}

/**
  * 销量排行：
  * 最近一天、七天，n 天，乘上一个权重
  * Created by MK33 on 2016/3/21.
  */
object HotSaleGoods {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)

    val sc = new SparkContext(sparkConf)
    val jedisPool = new JedisPool(new JedisPoolConfig, "10.201.128.216", 6379) with Serializable

    val r = sc.textFile("D:\\2016-03-21\\user_order_raw_data").map{ line =>
      val w = line.split("\t")
      //
      (w(3), w(4), w(6).toDouble, w(9), w(10))
    }.map{ case (goodsID, goodsName, sale_num, categoryID, category) =>
      (goodsID, (goodsName, sale_num, categoryID, category))
    }.reduceByKey((s1, s2) => (s1._1, s1._2 + s2._2, s1._3, s1._4))
        .map{ case (goodsID, (goodsName, sale_num, categoryID, category)) => {
          (categoryID, Seq((goodsID, sale_num)))
      }}.reduceByKey(_ ++ _).map{ case (category, seq) => (category, seq.sortWith(_._2 > _._2).take(20).map(_._1).mkString("#"))}
      .collect()
      .foreach{ case (category, ranking) => {
        val jedis = jedisPool.getResource
        jedis.set("rcmd_cate_hotsale_" + category, ranking)
        println("rcmd_cate_hotsale_" + category)
        jedis.close()
      }}


  }

}
