package com.bl.bigdata.useranalyze

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 计算用户的活跃度： 对 k 个物品产生过行为的用户数。
  * Created by MK33 on 2016/3/15.
  */
object UserActivity {

  def main(args: Array[String]) {
    if(args.length < 2){
      println("Please input <input path> and <output path>.")
      sys.exit(-1)
    }
    val inputPath = args(0)
    val outputPath = args(1)

    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    if(!inputPath.startsWith("/")) sparkConf.setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rawRDD = sc.textFile(inputPath).map(line => {
      // 提取字段
      val w = line.split("\t")
      // 用户ID, 商品ID, 商品类别
      (w(1), w(3), w(9))
    }).filter{ case (userID, goodsID, category) => userID.trim.length > 0}.distinct
    // 计算用户看过的商品数, 降序排列
    val userGoodsCount = rawRDD.map{ case (userID, goodsID, category) => (userID, 1)}.reduceByKey(_ + _).sortBy(_._2, false)
    // 计算用户浏览过的商品种类, 降序排列
    val userCategoryCount = rawRDD.map{ case (userID, goodsID, category) => (userID, category)}.distinct
      .map{ case (userID, category) => (userID, 1)}.reduceByKey(_ + _).sortBy(_._2, false)

    if (!outputPath.startsWith("/")) {
      println("===========  user_goods_count  ========")
      userGoodsCount.take(50).foreach(println)
      println("===========  user_category_count  =========")
      userCategoryCount.take(50).foreach(println)
    } else {
      userGoodsCount.saveAsTextFile(s"$outputPath/user_goods_count")
      userCategoryCount.saveAsTextFile(s"$outputPath/user_category_count")
    }
    sc.stop

  }
}
