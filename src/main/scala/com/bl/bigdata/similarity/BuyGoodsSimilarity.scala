package com.bl.bigdata.similarity

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 计算用户购买的物品在一级类目下的关联度
  * Created by MK33 on 2016/3/16.
  */
object BuyGoodsSimilarity {


  /**
    * @param args 用户行为记录文件路径，商品类目文件路径，结果保存的路径
    */
  def main(args: Array[String]): Unit ={

    if(args.length < 3){
      println("Pleas input <input path>, <input path> and <save path>.")
      sys.exit(-1)
    }

    val inputPath = args(0)
    val inputPath2 = args(1)
    val outPath = args(2)
    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    // 如果在本地测试，将 master 设置为 local 模式
    if (!inputPath.startsWith("/")) sparkConf.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val buyGoodsRDD = sc.textFile(inputPath)
      // 提取的字段: 商品类别,cookie,日期,用户行为编码,商品id
      .map( line => {
      //cookie ID, member id, session id, goods id, goods name, quality,
      // event data, behavior code, channel, category sid, dt
      val w = line.split("\t")
      // 商品类别,cookie,日期,用户行为编码,商品id
      (w(9), (w(0), w(6).substring(0, w(6).indexOf(" ")), w(7), w(3)))
    })
    // 提取购买的物品
      .filter{ case (category, (cookie, date, behaviorID, goodsID)) => behaviorID.equals("4000")}
      .map{case (category, (cookie, date, behaviorID, goodsID)) => (category, (cookie, date, goodsID))}.distinct

    val categoriesRDD = sc.textFile(inputPath2)
    // 提取字段
      .map( line => {
      val w = line.split("\t")
      // 商品的末级目录，一级目录
      (w(0), w(1))
    }).distinct

    val buyGoodsKindRDD = buyGoodsRDD.join(categoriesRDD)
      // 将商品的末级类别用一级类别替换
      .map{ case (category, ((cookie, date, goodsID), kind)) => ((cookie, date, kind), goodsID)}
    // 统计每种物品的购买数量
    val buyCount = buyGoodsKindRDD.map{ case ((cookie, date, kind), goodsID) => (goodsID, 1)}.reduceByKey(_ + _)

    // 计算用户购买物品在同一类别下的关联度
    val result = buyGoodsKindRDD.join(buyGoodsKindRDD)
      .filter{ case ((cookie, date, kind), (goodsID1, goodsID2)) => goodsID1 != goodsID2}
      .map{ case ((cookie, date, kind), (goodsID1, goodsID2)) => ((goodsID1, goodsID2), 1)}.reduceByKey(_ + _)
      .map{ case ((goodsID1, goodsID2), count) => (goodsID2, (goodsID1, count))}
      .join(buyCount)
      .map{ case (goodsID2, ((goodsID1, count), goodsID2Count)) => (goodsID1, goodsID2, count.toDouble / goodsID2Count)}

    if (!outPath.startsWith("/")) result.take(50).foreach(println)
    else result.saveAsTextFile(outPath)
















  }

}
