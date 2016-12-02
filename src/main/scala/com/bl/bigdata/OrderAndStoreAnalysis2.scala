package com.bl.bigdata

import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * Created by MK33 on 2016/10/19.
  */
object OrderAndStoreAnalysis2 {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("store order analysis"))


    val spark = new org.apache.spark.sql.hive.HiveContext(sc)

    val storeAndOrderDetailSql =
      """
        | SELECT address.storename, address.storelocation, address.district,
        | o.order_no, o.goods_code, o.goods_name, o.brand_sid, o.brand_name, o.sale_price, o.sale_sum,
        | o2.member_id,
        |  g2.category_id, g2.category_name
        |  FROM sourcedata.s03_oms_order_detail  o
        |  JOIN sourcedata.s03_oms_order o2 ON o2.order_no = o.order_no
        |  JOIN sourcedata.s03_oms_order_sub sub ON sub.order_no = o.order_no
        |  JOIN address_coordinate_store  address ON address.address = regexp_replace(sub.recept_address_detail, ' ', '')
        |  JOIN (SELECT DISTINCT g.sid, g.category_id, g.category_name  FROM recommendation.goods_avaialbe_for_sale_channel g ) g2 ON g2.sid = o.goods_code
        |  WHERE address.city = '上海市'  and o2.order_type_code <> '25' and o2.order_status = '1007'
      """.stripMargin

    import spark.implicits._
    val orderDetailDF = spark.sql(storeAndOrderDetailSql)
    orderDetailDF.cache()
    orderDetailDF.registerTempTable("order_spark")

    // 计算所有商品的人均消费次数的方差
    val goodsAvgCountDF = spark.sql(
      """
          select storename, storelocation, goods_code, goods_name, sum(sale_sum) sale_amt, count(distinct member_id) member_count, sum(sale_price)  sales
          from order_spark
          group by storename, storelocation, goods_code, goods_name
          having count(distinct member_id) > 10

        """.stripMargin)

      goodsAvgCountDF.registerTempTable("tmp2")

    val goodsAvg = spark.sql(
      """
        select goods_code, avg(sale_amt / member_count) a from tmp2 group by goods_code

      """.stripMargin).registerTempTable("tmp3")

    val deviationDF = spark.sql(
      """
         select  tmp2.goods_code, tmp2.goods_name, sqrt(sum(power(tmp3.a - tmp2.sale_amt / tmp2.member_count, 2)) / count(distinct tmp2.storename)) deviation
         from tmp2
         join tmp3 on tmp2.goods_code = tmp3.goods_code
         group by tmp2.goods_code, tmp2.goods_name
         order by deviation desc

      """.stripMargin)

    val goodsSalesPairFunction = new org.apache.spark.rdd.PairRDDFunctions(goodsAvgCountDF.rdd.map {
      row => ((row.getString(2), row.getString(3)), Seq((row.getString(0), row.getString(1), row.getDouble(4), row.getLong(5), row.getDouble(6))))
    }).reduceByKey(_ ++ _)
    val goodsDeviationPairFunction = new org.apache.spark.rdd.PairRDDFunctions(deviationDF.rdd.map { row =>
      ((row.getString(0), row.getString(1)), row.getDouble(2))
    })


    val goodsStoreDeviationRDD = goodsDeviationPairFunction.join(goodsSalesPairFunction)
      goodsStoreDeviationRDD.map { case ((goodsCode, goodsName), (deviation, goodsSales)) =>
          s"${goodsCode},${goodsName},${deviation},${goodsSales.map { case (storename, storelocation, saleAmt, memberCount, sales) =>
            s"$saleAmt#$memberCount#$sales"}.mkString(" || ")}"
      }.coalesce(1).saveAsTextFile("/tmp/goodsStoreDeviation2")



    deviationDF.rdd.map(row => row.getString(0) + "," + row.getString(1) + "," + row.getDouble(2)).coalesce(1).saveAsTextFile("/tmp/result5")


    spark.dropTempTable("tmp2")
    spark.dropTempTable("tmp3")



    // 计算品牌的人均购买次数的方差
    val brandDF = spark.sql(
      """
          select storename, storelocation, brand_sid, brand_name, sum(sale_sum) sale_amt, count(distinct member_id) member_count
          from order_spark
          group by storename, storelocation, brand_sid, brand_name

      """.stripMargin).registerTempTable("tmp4")

    val brandAvg = spark.sql(
      """
        select brand_sid, avg(sale_amt / member_count) a from tmp4 group by brand_sid

      """.stripMargin).registerTempTable("tmp5")

    val deviationBrand = spark.sql(
      """
         select  tmp4.brand_sid, tmp4.brand_name, sqrt(sum(power(tmp5.a - tmp4.sale_amt / tmp4.member_count, 2)) / count(distinct tmp4.storename)) deviation
         from tmp4
         join tmp5 on tmp4.brand_sid = tmp5.brand_sid
         group by tmp4.brand_sid, tmp4.brand_name
         order by deviation desc

      """.stripMargin)
    deviationBrand.rdd.map(row => row.getString(0) + "," + row.getString(1) + "," + row.getDouble(2)).coalesce(1).saveAsTextFile("/tmp/brand_result")



    // 计算品类的人均购买数量的方差, 人均购买次数（购买了此类商品的用户的人均购买数量、次数）
    // 品类的价格带
    val priceZone = spark.sql("select category_id, category_name, goods_id, goods_name, sale_price from recommendation.goods_avaialbe_for_sale_channel ")

    val categoryDF = spark.sql(
      """
          select storename, storelocation, category_id, category_name, count(distinct member_id) member_count, sum(sale_sum) sale_amt, sum(sale_sum * sale_price) sales
          from order_spark
          group by storename, storelocation, category_id, category_name

      """.stripMargin)
      categoryDF.registerTempTable("tmp6")

    val categoryAvg = spark.sql(
      """
        select category_id, avg(sale_amt / member_count) a from tmp6 group by category_id

      """.stripMargin)
      categoryAvg.registerTempTable("tmp7")

    val deviationCategory = spark.sql(
      """
         select  tmp6.category_id, tmp6.category_name, sqrt(sum(power(tmp7.a - tmp6.sale_amt / tmp6.member_count, 2)) / count(distinct tmp6.storename)) deviation,
                 sum(tmp6.member_count) member_count, sum(sale_amt) sale_amt, sum(sales)  sales
         from tmp6
         join tmp7 on tmp7.category_id = tmp6.category_id
         group by tmp6.category_id, tmp6.category_name
         order by deviation desc

      """.stripMargin)

    val categoryDeviationPairRDDFunction = new org.apache.spark.rdd.PairRDDFunctions(deviationCategory.rdd.map{ row =>
      (((row.getLong(0).toString, row.getString(1)), (row.getDouble(2), row.getLong(3), row.getDouble(4), row.getDouble(5))))
    })

    val categoryPairRDDFunction = new org.apache.spark.rdd.PairRDDFunctions[(String, String), String](categoryDF.rdd.map { row =>
      ((if (row.isNullAt(2)) "null" else row.getLong(2).toString, row.getString(3)), row.getString(0) + "#" + row.getString(1) + "#" + row.getLong(4) + "#" + row.getDouble(5) + "#" + row.getDouble(6))
    }).reduceByKey(_ + " || " + _)

    categoryDeviationPairRDDFunction.join(categoryPairRDDFunction).map { case ((categoryID, categoryName), (deviation, stores)) =>
      s"$categoryID,$categoryName,${deviation._1},${deviation._2},${deviation._3},${deviation._4},${stores.split(" \\|\\| ").length.toDouble / 220},$stores"
    }.coalesce(1).saveAsTextFile("/tmp/category_deviation5")


//    spark.dropTempTable()
    deviationCategory.rdd.map(row => row.getLong(0) + "," + row.getString(1) + "," + row.getDouble(2)).coalesce(1).saveAsTextFile("/tmp/category_result1")








  }



}
