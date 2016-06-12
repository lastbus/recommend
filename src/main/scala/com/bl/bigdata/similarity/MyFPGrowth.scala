package com.bl.bigdata.similarity

import com.bl.bigdata.util.{ToolRunner, ConfigurationBL, Tool}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by MK33 on 2016/3/31.
  */
class MyFPGrowth extends Tool{
  override def run(args: Array[String]): Unit = {
    val input = ConfigurationBL.get("user.behavior.raw.data")
    val output = ConfigurationBL.get("", "local")
    val local = output.contains("local")
    val redis = output.contains("redis")

    val sparkConf = new SparkConf().setAppName("FP-Growth")
    if (local) sparkConf.setMaster("local[*]")
    if (redis)
      for ((k, v) <- ConfigurationBL.getAll)
        sparkConf.set(k, v)

    val sc = new SparkContext(sparkConf)

    val rawRDD = sc.textFile(input).map(_.split("\t"))
      .map(array => (array(0), array(7), array(3)))
      .filter(tuple => tuple._2.equals("4000")).map(s => (s._1, Seq(s._3))).reduceByKey(_ ++ _).map(_._2.toArray.distinct)

//    rawRDD.first().foreach(println)
    new FPGrowth().setMinSupport(0.0001).run(rawRDD).freqItemsets.filter(_.items.length > 1).collect().foreach { itemset =>
    println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)}
  }
}

object MyFPGrowth {

  def main(args: Array[String]) {
    val fPGrowth = new MyFPGrowth with ToolRunner
    fPGrowth.run(args)


  }
}
