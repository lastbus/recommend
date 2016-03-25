package com.bl.bigdata.similarity

import com.bl.bigdata.util.{ToolRunner, Tool}
import org.apache.spark.{SparkConf, SparkContext}
import com.redislabs.provider.redis._

/**
  * 计算用户浏览的某类商品之间的相似度
  * 计算方法：
  * 根据用户在某一天内浏览商品的记录去计算相似度。
  * r(A,B) = N(A,B) / N(B)
  * r(B,A) = N(A,B) / N(A)
  * Created by MK33 on 2016/3/14.
  */
class GoodsSimilarity extends Tool {

  def isEmpty(s: String): Boolean = {
    if(s.trim.length == 0) true
    else if(s.equalsIgnoreCase("NULL")) true
    else false
  }

  override def run(args: Array[String]): Unit = {
    if(args.length < 2) {
      println("There are at least 2 parameters: <input path> and <save path>.")
      sys.exit(-1)
    }
    val inputPath = args(0).trim
    val savePath = args(1).trim

    val sparkConf = new SparkConf().setAppName("look and look")
    if(!inputPath.startsWith("/")) sparkConf.setMaster("local[*]")
    sparkConf.set("redis.host", "10.201.128.216")
    sparkConf.set("redis.port", "6379")
    sparkConf.set("redis.timeout", "10000")

    val sc = new SparkContext(sparkConf)

    val rawRdd = sc.textFile(inputPath)
      // 提取需要的字段
      .map( line => {
      //cookie ID, member id, session id, goods id, goods name, quality,
      // event data, behavior code, channel, category sid, dt
      val w = line.split("\t")
      // cookie,商品类别,日期,用户行为,商品id
      (w(0), w(9), w(6).substring(0, w(6).indexOf(" ")), w(7), w(3))
    })
      // 提取 user 浏览商品的行为
      .filter{ case (cookie, category, date, behaviorId, goodsId) => behaviorId == "1000"}
      .map{ case (cookie, category, date, behaviorId, goodsId) =>
        ((cookie, category, date), goodsId)
      }.distinct.filter(v => !isEmpty(v._1._2))

    //    rawRdd.filter(_._2 == "159431").collect().foreach(println)

    // 将用户看过的商品两两结合在一起
    val tuple = rawRdd.join(rawRdd).filter{ case (k, (v1, v2)) => v1 != v2}
      .map{ case (k, (goodId1, goodId2)) => (goodId1, goodId2)}


    // 计算浏览商品 (A,B) 的次数
    val tupleFreq = tuple.map((_, 1)).reduceByKey(_ + _)
    // 计算浏览每种商品的次数
    val good2Freq = rawRdd.map(_._2).map((_, 1)).reduceByKey(_ + _)

    val good1Good2Similarity = tupleFreq.map{ case ((good1, good2), freq) => (good2, (good1, freq))}
      .join(good2Freq)
      .map{ case (good2, ((good1, freq), good2Freq)) =>  (good1, Seq((good2, freq.toDouble / good2Freq)))}
      .reduceByKey(_ ++ _)
      .mapValues(v => v.sortWith(_._2 > _._2).take(20))
      .map{ case (goods1, goods2) => ("rcmd_view_" + goods1, goods2.map(_._1).mkString("#"))}

    // 保存到 redis 中
    sc.toRedisKV(good1Good2Similarity)

    sc.stop()

  }
}

object GoodsSimilarity {

  def main(args: Array[String]) {
    (new GoodsSimilarity with ToolRunner).run(args)
  }
}