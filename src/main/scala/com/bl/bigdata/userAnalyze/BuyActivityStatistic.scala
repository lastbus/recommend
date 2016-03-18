package com.bl.bigdata.userAnalyze

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * 1. 统计上午、下午、晚上 购买类目top 10
  * 2. 订单里面分类目单价低于20元top20商品
  * 3. 分析订单里面购买关联类目，哪些类目同时购买
  *
  * 上午： 08-12
  * 下午：12-18
  * 晚上：其余
  * Created by MK33 on 2016/3/18.
  */
object BuyActivityStatistic {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("please input <input path> and <output path>.")
      sys.exit(-1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    if(!inputPath.startsWith("/")) {
      sparkConf.setMaster("local[*]")
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)
    }
    val sc = new SparkContext(sparkConf)

    val rawRDD = sc.textFile(inputPath).map(line => {
      val attr = line.split("\t")
      // 类目  时间  行为编码 价格
      (attr(9),attr(6).substring(attr(6).indexOf(" ") + 1), attr(7))
    })
      // 购买记录
      .filter{case (category, time, behavior) => behavior.equals("4000")}
      .map{ case (category, time, behavior) => (category, time)}

    // 统计上午、下午、晚上 购买类目top 10
    val morningRDD = rawRDD
                    .filter{ case (category, time) => time >= "08" & time <= "12:00:00.0"}
                    .map{ s => (s._1, 1)}
                    .reduceByKey(_ + _)
                    .collect().sortWith(_._2 >= _._2)
                    .take(10)
                    .foreach(println)
    println("============")
    val noonRDD = rawRDD
                  .filter{ case (category, time) => time >= "12" & time <= "18:00:00.0"}
                  .map{ s => (s._1, 1)}
                  .reduceByKey(_ + _)
                  .collect()
                  .sortWith(_._2 >= _._2)
                  .take(10)
                  .foreach(println)
    println("============")
    val nightRDD = rawRDD
                   .filter{ case (category, time) => time >= "18" | time <= "08"}
                   .map{ s => (s._1, 1)}
                   .reduceByKey(_ + _)
                   .collect()
                   .sortWith(_._2 >= _._2)
                   .take(10)
                   .foreach(println)
  }

}
