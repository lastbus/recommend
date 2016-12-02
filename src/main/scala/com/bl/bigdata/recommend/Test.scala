package com.bl.bigdata.recommend

import com.infomatiq.jsi.Point
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

/**
  * Created by MK33 on 2016/10/13.
  */
object Test {
  def main(args: Array[String]) {
//    Logger.getRootLogger.setLevel(Level.WARN)
    val longitude = 121.443823
    val latitude = 31.265052
    val point = new Point(longitude.toFloat, latitude.toFloat)

    val sc = new SparkContext(new SparkConf().set("spark.executor.memory", "2g").setAppName("order analysis"))
    // name, address, longitude/latitude, store zone
    val storeRawRDD = sc.textFile("hdfs://m78sit:8020/tmp/store").map(s=>{ val ws = s.split("\t"); (ws(0), ws(1), ws(2), ws(3))}).distinct().collect()
    val shopInfo = new ShopInfo(storeRawRDD)
    val orderRawRdd = sc.textFile("hdfs://m78sit:8020/tmp/order_store_lng_lat").map(s => s.substring(1, s.length - 2).split("\t"))
//    orderRawRdd.map(_.length).distinct().foreach(println)
//    Console print orderRawRdd.first().mkString("\n")
    orderRawRdd.take(10).map
    { case Array(distinct, longitude, latitude, salePrice, saleSum, goodsCode, goodsName, cateSid, shopId, shopName, brandSid, brandName, orderNo, memberId) =>
      val point = new Point(longitude.toFloat, latitude.toFloat)
      val tmp = shopInfo.getStores(point)
      val a = for ((a,b,c) <- tmp) yield (a,b,c,distinct, longitude, latitude, salePrice, saleSum, goodsCode, goodsName, cateSid, shopId, shopName, brandSid, brandName, orderNo, memberId)
      a
    }.flatMap(s=> s).foreach(println)


//    val order = orderRawRdd.filter(_.size == 14).filter(s => s(0).contains("浦东")).collect()
//      .map { case Array(distinct, longitude, latitude, salePrice, saleSum, goodsCode, goodsName, cateSid, shopId, shopName, brandSid, brandName, orderNo, memberId) =>
//    val point = new Point(longitude.toFloat, latitude.toFloat)
//    val tmp = shopInfo.getStores(point)
//    val a = for ((a,b,c) <- tmp) yield (a,b,c,distinct, longitude, latitude, salePrice, saleSum, goodsCode, goodsName, cateSid, shopId, shopName, brandSid, brandName, orderNo, memberId)
//    a
//    }.flatMap(s => s)
//
//    sc.parallelize(order).saveAsTextFile("hdfs://m78sit:8020/tmp/hhh")


  }

}
