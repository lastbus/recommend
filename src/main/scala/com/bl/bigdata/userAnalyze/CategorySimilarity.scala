package com.bl.bigdata.userAnalyze

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 计算某一天用户购买的物品种类相似度
  * 物品AB的相似度计算公式：
  * N(AB)/(N(A)*N(B))
  * N(AB)：同时购买 A 和 B 的用户数
  * Created by MK33 on 2016/3/18.
  */
object CategorySimilarity {

  def main(args: Array[String]): Unit ={
    if(args.length < 2) {
      println("please input <input path> and <output path>.")
      sys.exit(-1)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    if (!inputPath.startsWith("/")) sparkConf.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val rawRDD = sc.textFile(inputPath).map( line => {
      val attr = line.split("\t")
      // member ID, 时间，类目 行为编码
      ((attr(1), attr(6).substring(0, 10)), attr(9), attr(7))
    }).filter{ case ((cookieID, date), category, behavior) => behavior.equals("4000") && !category.equalsIgnoreCase("null")}
        .map{ case (key, value1, value2) => (key, value1)}

    val r = rawRDD.join(rawRDD).map{ case ((cookieID, date), (category1, category2)) => (category1, category2)}
      .filter{ case (category1, category2) => !category1.equals(category2)}


  }
}
