package com.bl.bigdata.similarity

import org.apache.spark.{SparkContext, SparkConf}
import com.redislabs.provider.redis._

/**
  * 计算用户浏览的物品和购买的物品之间的关联度
  * 计算方法：一天之内用户浏览和购买物品的记录。
  * r(seeGoods, buyGoods) = N(seeGoods, buyGoods) / N(buyGoods)
  * Created by MK33 on 2016/3/15.
  */
object SeeBuyGoodsSimilarity {

  def main(args: Array[String]): Unit = {

    if(args.length < 2){
      println("Pleas input <input path> and <save path>.")
      sys.exit(-1)
    }

    val inputPath = args(0)
    val outPath = args(1)
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    sparkConf.set("redis.host", "10.201.128.216")
    sparkConf.set("redis.timeout", "10000")
    // 如果在本地测试，将 master 设置为 local 模式
    if (!inputPath.startsWith("/")) sparkConf.setMaster("local")
    val sc = new SparkContext(sparkConf)

    val rawData = sc.textFile(inputPath)
      // 提取需要的字段
      .map( line => {
      //cookie ID, member id, session id, goods id, goods name, quality,
      // event data, behavior code, channel, category sid, dt
      val w = line.split("\t")
      // cookie,商品类别,日期,用户行为编码,商品id
      ((w(0), w(9), w(6).substring(0, w(6).indexOf(" "))), w(7), w(3))
    })

    // 用户浏览的商品
    val browserRdd = rawData.filter{ case ((cookie, category, date), behavior, goodsID) => behavior.equals("1000")}
      .map{ case((cookie, category, date), behavior, goodsID) => ((cookie, category, date), goodsID)}.distinct
    // 用户购买的商品
    val buyRdd = rawData.filter{ case ((cookie, category, date), behavior, goodsID) => behavior.equals("4000")}
      .map{ case((cookie, category, date), behavior, goodsID) => ((cookie, category, date), goodsID)}.distinct
    // 用户购买商品数量的统计
    val buyCountRDD = buyRdd.map{ case ((cookie, category, date), goodsID) => (goodsID, 1)}
      .reduceByKey(_ + _)

    // 计算用户浏览和购买的商品之间的相似性：（浏览的商品A， 购买的商品B）的频率除以 B 的频率
    val browserAndBuy = browserRdd.join(buyRdd)
      .map{ case ((cookie, category, date), (goodsIDBrowser, goodsIDBuy)) => ((goodsIDBrowser, goodsIDBuy), 1)}
      .reduceByKey(_ + _)
      .map{ case ((goodsIDBrowser, goodsIDBuy), count) => (goodsIDBuy, (goodsIDBrowser, count))}
      .join(buyCountRDD)
      .map{ case (goodsIDBuy, ((goodsIDBrowser, count), buyCount)) => (goodsIDBrowser, goodsIDBuy, count.toDouble / buyCount)}
      .map{ case (goodsBrowser, goodsBuy, relation) => (goodsBrowser, Seq((goodsBuy, relation)))}
      .reduceByKey(_ ++ _)
      .map(s => ("rcmd_shop_" + s._1, s._2.sortWith(_._2 > _._2).take(20).map(_._1).mkString("#")))

    sc.toRedisKV(browserAndBuy)
    // 如果是本地运行，则直接输出，否则保存在 hadoop 中。
    if (!inputPath.startsWith("/")) browserAndBuy.take(50).foreach(println)
    else sc.toRedisKV(browserAndBuy)
//    browserAndBuy.first()

    sc.stop()
  }

}
