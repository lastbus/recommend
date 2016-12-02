package com.bl.bigdata

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 统计一级二级类目商品特征
  * Created by MK33 on 2016/10/21.
  */
object OrderAndStoreAnalysis4 {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("store order analysis"))


    val spark = new org.apache.spark.sql.hive.HiveContext(sc)

    val storeAndOrderDetailSql =
      """
        | SELECT distinct address.storename, address.storelocation, address.district,
        |        o.order_no, o.goods_code, o.goods_name, o.brand_sid, o.brand_name, o.sale_price, o.sale_sum,
        |        o2.member_id,
        |        g2.level1_id, g2.level1_name
        |  FROM sourcedata.s03_oms_order_detail  o
        |  JOIN sourcedata.s03_oms_order o2 ON o2.order_no = o.order_no
        |  JOIN sourcedata.s03_oms_order_sub sub ON sub.order_no = o.order_no
        |  JOIN address_coordinate_store  address ON address.address = regexp_replace(sub.recept_address_detail, ' ', '')
        |  JOIN (
        |  SELECT DISTINCT   g.sid, c.level1_id, c.level1_name
        |  FROM idmdata.dim_management_category c
        |  JOIN recommendation.goods_avaialbe_for_sale_channel g ON g.pro_sid = c.product_id  ) g2 ON g2.sid = o.goods_code
        |  WHERE address.city = '上海市'  and o2.order_type_code <> '25' and o2.order_status = '1007'
      """.stripMargin

    import spark.implicits._
    val orderDetailDF = spark.sql(storeAndOrderDetailSql)
    orderDetailDF.cache()
    orderDetailDF.registerTempTable("order_spark")

    // 每个门店周边会员数
    spark.sql(
      """
        |select storename, storelocation, count(distinct member_id) from order_spark group by storename, storelocation

      """.stripMargin).collect().foreach(println)

    // 各个门店下面，每个品类的消费金额，消费数量，订单数（购买次数）
    val a = spark.sql(
      """
        | select storename, storelocation, level1_id, level1_name, sum(sale_price * sale_sum), sum(sale_sum), count(distinct order_no)
        | from order_spark group by storename, storelocation, level1_id, level1_name
        |
      """.stripMargin)

    val categoryies = orderDetailDF.select("level1_id", "level1_name").rdd.distinct().collect().map(r=> ((r.getLong(0).toString, r.getString(1)), "null")).sorted.toMap

    // 计算每个门店下，各个品类的消费金额、消费商品数量、订单数占比(还是计算出来绝对值然后导入excel，然后再分析比较好吧）
    val a1 = a.rdd.map(row => ((row.getString(0), row.getString(1)),Seq((row.getLong(2).toString, row.getString(3), row.getDouble(4), row.getDouble(5), row.getLong(6))))).
      reduceByKey(_ ++ _).
      map { case ((storename, storelocation), goods) =>
          val sum = goods.foldLeft((0.0, 0.0, 0L))((a: (Double, Double, Long), b: (String, String, Double, Double, Long)) => (a._1 + b._3, a._2 + b._4, a._3 + b._5))
        val gg = goods.map { case (level, levelName, price, goodsCount, orderNumber) =>
          ((level, levelName), price / sum._1 + "#" + goodsCount / sum._2 + "#" + orderNumber.toDouble / sum._3)
        }.toMap
        val map = scala.collection.mutable.Map[(String, String), String]()
        for (m <- categoryies.keys) {
          map(m) = gg.getOrElse(m, "0.0")
        }
        (storename, storelocation, map.toArray)
      }

    a1.map {  case (sn ,sl, goods) =>
      s"$sn,$sl,${goods.sortBy(_._1).map(s => s._1._1 + "#" + s._1._2 + "#" + s._2).mkString(",")}"
    }.coalesce(1).saveAsTextFile("/tmp/a5")

    // 计算订单数、消费金额、消费数量绝对值，然后导入excel 分析吧

    val absValue = a.rdd.map(row => ((row.getString(0), row.getString(1)),Seq((row.getLong(2).toString, row.getString(3), row.getDouble(4), row.getDouble(5), row.getLong(6))))).
      reduceByKey(_ ++ _).
      map { case ((storename, storelocation), goods) =>
        val gg = goods.map(s => ((s._1, s._2), s._3 + "#" + s._4 + "#" + s._5)).toMap
        val map = scala.collection.mutable.Map[(String, String), String]()
        for (m <- categoryies.keys) {
          map(m) = gg.getOrElse(m, "not-a-value")
        }
        (storename, storelocation, map.toArray.sortBy(_._1).map(s => s._1._1 + "#" + s._1._2 + "#" + s._2).mkString(","))
      }

    absValue.coalesce(1).saveAsTextFile("/tmp/a7")



    import org.apache.spark.sql.functions._
    val salesAndAmt = orderDetailDF.filter($"storename" === "世纪联华中环百联店" || $"storename" === "华联超市重庆北店").
      groupBy("storename", "level1_name").
      agg(sum($"sale_price" * $"sale_sum"), countDistinct("order_no")).
      sort($"storename", sum(($"sale_price" * $"sale_sum")).desc)
    salesAndAmt.show()














  }

}
