package com.bl.bigdata.similarity

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 计算 一个用户、在某一天、浏览某类商品 之间的相似度
  * Created by MK33 on 2016/3/14.
  */
object GoodsSimilarity {

  /**
    * 输入的参数：
    *  1 输入文件 路径: 本地或者 hdfs
    *  2 结果保存的路径
    *
    * @param args  程序参数
    */
  def main(args: Array[String]): Unit = {
    if(args.length < 2) {
      println("There are at least 2 parameters: <input path> and <save path>.")
      sys.exit(-1)
    }
    val inputPath = args(0).trim
    val savePath = args(1).trim

    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    if(!inputPath.startsWith("/")) sparkConf.setMaster("local")
    val sc = new SparkContext(sparkConf)

    val rawRdd = sc.textFile(inputPath)
      // 提取需要的字段
      .map( line => {
        //cookie ID, member id, session id, goods id, goods name, quality,
        // event data, behavior code, channel, category sid, dt
        val w = line.split("\t")
        // cookie,商品类别,日期,商品id
        (w(0), w(9), w(6).substring(0, w(6).indexOf(" ")), w(7), w(3))
      })
      // 提取 user 浏览商品的行为
      .filter{case (cookie, category, date, behaviorId, goodsId) => behaviorId == "1000"}
      .map{case (cookie, category, date, behaviorId, goodsId) =>
        ((cookie, category, date), goodsId)
      }.distinct

      // 将用户看过的商品两两结合在一起
    val tuple = rawRdd.join(rawRdd).filter{case (k, (v1, v2)) => v1 != v2}
        .map{case (k, (goodId1, goodId2)) => (goodId1, goodId2)}


    val tupleFreq = tuple.map((_, 1)).reduceByKey(_ + _)
    val good2Freq = rawRdd.map(_._2).map((_, 1)).reduceByKey(_ + _)

    val good1Good2Similarity = tupleFreq.map{case ((good1, good2), freq) => (good2, (good1, freq)) }
      .join(good2Freq).map{case (good2, ((good1, freq), good2Freq)) => { (good1, good2, freq.toDouble / good2Freq)}}
      .map{case (good1, good2, similarity) => s"$good1#$good2:$similarity"}
//      good1Good2Similarity.take(100).foreach(println)

    good1Good2Similarity.saveAsTextFile(savePath)

  }

}
