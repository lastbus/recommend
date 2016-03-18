package com.bl.bigdata.similarity

import org.apache.spark.{SparkContext, SparkConf}

/**
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
    val buyCount = buyRdd.map{ case ((cookie, category, date), goodsID) => (goodsID, 1)}
      .reduceByKey(_ + _)

    // 计算用户浏览和购买的商品之间的相似性：（浏览的商品A， 购买的商品B）的频率除以 B 的频率
    val browserAndBuy = browserRdd.join(buyRdd)
      .map{ case ((cookie, category, date), (goodsIDBrowser, goodsIDBuy)) => ((goodsIDBrowser, goodsIDBuy), 1)}
      .reduceByKey(_ + _)
      .map{ case ((goodsIDBrowser, goodsIDBuy), count) => (goodsIDBuy, (goodsIDBrowser, count))}
      .join(buyCount).map{case (goodsIDBuy, ((goodsIDBrowser, count), buyCount)) => (goodsIDBrowser, goodsIDBuy, count.toDouble / buyCount)}

    // 如果是本地运行，则直接输出，否则保存在 hadoop 中。
    if (!inputPath.startsWith("/")) browserAndBuy.take(50).foreach(println)
    else {
      browserAndBuy.saveAsTextFile(outPath)
    }

    sc.stop()
  }

}
