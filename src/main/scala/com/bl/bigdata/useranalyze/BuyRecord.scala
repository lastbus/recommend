package com.bl.bigdata.useranalyze

import com.bl.bigdata.util.{ToolRunner, ConfigurationBL, Tool}
import org.apache.spark.{SparkContext, SparkConf}
import com.redislabs.provider.redis._

/**
  * Created by MK33 on 2016/3/31.
  */
class BuyRecord extends Tool {

  override def run(args: Array[String]): Unit = {

    val inputPath = ConfigurationBL.get("user.behavior.raw.data")
    val outPath = ConfigurationBL.get("recmd.output")
    val redis = outPath.contains("redis")
    val local = outPath.contains("local")

    val sparkConf = new SparkConf().setAppName("购买记录")
    if (local) sparkConf.setMaster("local[*]")
    if (redis) {
      for ((key, value) <- ConfigurationBL.getAll if key.startsWith("redis."))
        sparkConf.set(key, value)
    }
    val sc = new SparkContext(sparkConf)
    val accumulator = sc.accumulator(0)
    val rawRDD = sc
      .textFile(inputPath).map(_.split("\t"))
      .map(array => (array(1), array(7), (array(3), array(6).substring(0, array(6).indexOf(" "))), array(9)))
      .filter(_._2.equals("4000"))
      .map(s => (s._1, Seq((s._3, s._4)))) // 商品ID，商品种类
      .reduceByKey((s1, s2) => {accumulator += 1; s1 ++ s2})
      .map(item => ("rcmd_memid_shop_" + item._1, format(item._2)))

    if (redis) sc.toRedisKV(rawRDD)
    if (local)
      rawRDD.take(50).foreach(println)

  }

  /** goodsID, categoryID, time*/
  def format(array: Seq[((String, String), String)]): String = {
    array.groupBy(_._2).map(s => s._1 + ":" + s._2.distinct.map(_._1).sortWith(_._2 > _._2).map(_._1).mkString(",")).mkString("#")
  }

}

object BuyRecord {

  def main(args: Array[String]) {

    val buyRecord = new BuyRecord with ToolRunner
    buyRecord.run(args)
    val categoryGoodsID = Array((("c10" ,"1"), "g1"), (("c20" ,"0"), "g2"), (("c3" ,""), "g3"),
                                (("c12" ,"0"), "g1"), (("c22" ,"78"), "g2"), (("c3" ,""), "g3"),
                                (("c13"  ,"2"), "g1"), (("c2" ,"5"), "g2"), (("c3" ,""), "g3"))

    Console println buyRecord.format(categoryGoodsID)


  }
}
