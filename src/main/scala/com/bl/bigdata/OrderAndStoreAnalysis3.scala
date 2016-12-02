package com.bl.bigdata

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK33 on 2016/10/20.
  */
object OrderAndStoreAnalysis3 {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("store order analysis"))


    val spark = new org.apache.spark.sql.hive.HiveContext(sc)

    val storeAndOrderDetailSql =
      """
        | SELECT address.storename, address.storelocation, address.district,
        | o.order_no, o.goods_code, o.goods_name, o.brand_sid, o.brand_name, o.sale_price, o.sale_sum,
        | o2.member_id,
        |  g2.level1_id, g2.level1_name
        |  FROM sourcedata.s03_oms_order_detail  o
        |  JOIN sourcedata.s03_oms_order o2 ON o2.order_no = o.order_no
        |  JOIN sourcedata.s03_oms_order_sub sub ON sub.order_no = o.order_no
        |  JOIN address_coordinate_store  address ON address.address = regexp_replace(sub.recept_address_detail, ' ', '')
        |  JOIN (
        |  SELECT DISTINCT   g.sid, c.level1_id, c.level1_name
        |  FROM idmdata.dim_management_category c
        |  JOIN recommendation.goods_avaialbe_for_sale_channel g ON g.pro_sid = c.product_id  ) g2 ON g2.sid = o.goods_code
        |  WHERE address.city = '上海市'  and o2.order_type_code <> '25' and o2.order_status = '1007'
        |
      """.stripMargin

    import spark.implicits._
    val orderDetailDF = spark.sql(storeAndOrderDetailSql)
    orderDetailDF.cache()
    orderDetailDF.registerTempTable("order_spark")

    val (salesSum, salesAmt) = spark.sql("select sum(sale_price * sale_sum), sum(sale_sum) from order_spark ").rdd.map(r => (r.getDouble(0), r.getDouble(1))).take(1)(0)

    spark.sql(
      """
        select storename, count(distinct member_id) c from order_spark group by storename

      """.stripMargin).registerTempTable("memberTable")

    // 每个门店周围人购买此类商品的比例的平均值
    val a = spark.sql(
      """
         select category_id, category_name, avg(a0 / m.c) a1
         from
         (
        select o.storename, category_id, category_name, count(goods_code)  a0
        from order_spark o
        group by o.storename, category_id, category_name
        ) tt
        join memberTable m on m.storename = tt.storename
        group by category_id, category_name

      """.stripMargin)

    a.registerTempTable("t1")
    a.cache()
    a.select("a1").rdd.map(_.getDouble(0)).collect().max

    // 每个门店周围人购买此类商品数量占比的平均值
    spark.sql(
      """
        select storename, sum(sale_sum) c from order_spark group by storename
      """.stripMargin).registerTempTable("saleAmtTable")
    val b = spark.sql(
      """
        select category_id, category_name, avg(b0 / m.c) a1
        from (
          select o.storename, category_id, category_name, sum(sale_sum) b0
          from order_spark o
          group by o.storename, category_id, category_name
         ) tt
         join saleAmtTable m on m.storename = tt.storename
         group by category_id, category_name
      """.stripMargin)
    b.registerTempTable("t2")
    b.cache()
    b.select("a1").rdd.map(_.getDouble(0)).collect().max

    // 每个门店周围人购买此类商品金额占比
    spark.sql(
      """
        select storename, sum(sale_sum * sale_price) c from  order_spark group by storename
      """.stripMargin).registerTempTable("priceTable")


    val c = spark.sql(
      """
        select  category_id, category_name, avg(b0 / m.c) a1
        from (
          select o.storename, category_id, category_name, sum(sale_sum * sale_price) b0
          from order_spark o
          group by o.storename, category_id, category_name
        ) tt
        join priceTable m on m.storename = tt.storename
        group by category_id, category_name
      """.stripMargin)
    c.registerTempTable("t3")
    c.cache()
    c.select("a1").rdd.map(_.getDouble(0)).collect().max

    // 销售金额在所有门店中的占比
    val d = spark.sql(
      s"""
        select category_id, category_name, sum(sale_sum * sale_price) / ${salesSum} a1
        from  order_spark o
        group by category_id, category_name
      """.stripMargin)
    d.registerTempTable("t4")

    d.cache()
    d.select("a1").rdd.map(_.getDouble(0)).collect().max

    // 销售数量在所有门店中的占比
    val e = spark.sql(
      s"""
        select category_id, category_name, sum(sale_sum) / ${salesAmt} a1
        from  order_spark o
        group by category_id, category_name
      """.stripMargin)
    e.registerTempTable("t5")
    e.cache()
    e.select("a1").rdd.map(_.getDouble(0)).max

    val org.apache.spark.sql.Row(categoryCount) = spark.sql("select count(distinct storename) from order_spark ").take(1)(0)

    val f = spark.sql(
      s"""
        select category_id, category_name, count(distinct storename) / ${categoryCount} a1 from order_spark group by category_id, category_name
      """.stripMargin)
    f.registerTempTable("t6")
    f.cache()
    f.select("a1").rdd.map(_.getDouble(0)).max

    val vector = spark.sql(
      """
        select t1.category_id, t1.category_name, t1.a1, t2.a1, t3.a1, t4.a1, t5.a1, t6.a1
        from t1
        join t2 on t1.category_id = t2.category_id
        join t3 on t1.category_id = t3.category_id
        join t4 on t1.category_id = t4.category_id
        join t5 on t1.category_id = t5.category_id
        join t6 on t1.category_id = t6.category_id
      """.stripMargin)


    // Load and parse the data
    val data = sc.textFile("file:///opt/spark-2.1.0-bin-bl-spark-2.6.0/data/mllib/kmeans_data.txt")
    val parsedData = data.map(s => org.apache.spark.mllib.linalg.Vectors.dense(s.split(' ').map(_.toDouble))).cache()


    val tranData = vector.rdd.map{ row => ((row.getLong(0).toString, row.getString(1)),
      org.apache.spark.mllib.linalg.Vectors.dense(row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getDouble(5), row.getDouble(6))) }

    val tt = tranData.map(_._2).cache()
    // Cluster the data into two classes using KMeans
    val numClusters = 3
    val numIterations = 50
    val runs = 10
    val clusters = org.apache.spark.mllib.clustering.KMeans.train(tt, numClusters, numIterations, runs)
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(tt)


    val localResult = tranData.collect().map(s => (s._1, clusters.predict(s._2), s._2))
    tranData.collect().map(s => (clusters.predict(s._2), s._1)).groupBy(_._1).foreach(s => println(s._1 + " " + s._2.length))
    localResult.filter(_._2 == 0).foreach(println)
    localResult.filter(_._2 == 1).foreach(println)
    localResult.filter(_._2 == 2).foreach(println)
    val categoryLocal = Map[(String, String), String]()

    val categoryDF = spark.sql(
      """
          select storename, storelocation, category_id, category_name, count(distinct member_id) member_count, sum(sale_sum) sale_amt, sum(sale_sum * sale_price) sales
          from order_spark
          group by storename, storelocation, category_id, category_name

      """.stripMargin)
    categoryDF.registerTempTable("tmp6")


    val df = tranData.map(s => (s._1._1, s._1._2, clusters.predict(s._2), s._2(0), s._2(1), s._2(2), s._2(3), s._2(4))).
      toDF("category_id", "category_name", "label", "s1", "s2", "s3", "s4", "s5")
      df.save("/tmp/tt")
    df.registerTempTable("resultTable")
    df.map(r => r.toSeq)







    println("Within Set Sum of Squared Errors = " + WSSSE)

    // Save and load model
//    clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
//    val sameModel = org.apache.spark.mllib.clustering.KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")









  }

}
