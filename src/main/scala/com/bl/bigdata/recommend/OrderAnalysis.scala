package com.bl.bigdata.recommend

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 分析订单数据特征, 初步分析 ，sql 在spark sql 中执行
  * Created by MK33 on 2016/10/11.
  */
object OrderAnalysis {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("order analysis"))
//    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val spark = new HiveContext(sc)

    val orderSql = " SELECT a.district, a.longitude, a.latitude, o.sale_price, o.sale_sum, o.goods_code, o.goods_name, o.cate_sid,  " +
                    "  o.shop_sid, o.shop_name, o.brand_sid, o.brand_name, o.order_no, order.member_id  " +
      " FROM s03_oms_order_detail  o  " +
      " left join sourcedata.s03_oms_order order on o.order_no = order.order_no" +
      "  join sourcedata.s03_oms_order_sub o2 on o.order_no = o2.order_no  " +
      "  join addressCoordinate a on a.address = regexp_replace(o2.recept_address_detail, ' ', '')  " +
      "  where o2.recept_address_detail is not NULL AND a.province = '上海市' "

    val orderRawDF = spark.sql(orderSql)
//      orderRawDF.createOrReplaceTempView("order")

    // 将订单中的经纬度匹配 快到家 门店配送范围
    orderRawDF.rdd.map( r => (r.getString(0), r.getString(1), r.getString(2)))


    // 1  消费总量分析： 区域消费总量
    val membersDF = spark.sql("select district, count(distinct member_id) as member_sum from order group by district ")
//    membersDF.createOrReplaceTempView("member")

    val consumerSum = spark.sql("select district, sum(sale_price * sale_sum) as sum , sum(sale_sum) as amt , " +
      "sum(sale_price * sale_sum) / sum(sale_sum) as avg, sum(sale_price * sale_sum) / count(distinct order_no), " +
      " count(member_id) / count(distinct member_id),  sum(sale_price * sale_sum) / count(distinct member_id), count(distinct member_id)  " +
      " from order  group by district order by amt desc")

    val avgConsumer = spark.sql("select district, count(distinct order_no)    ")


    // 区域品类消费总量分析
    val categoryConsumerSum = spark.sql("select district, cate_sid, sum(sale_price * sale_sum) as sum , sum(sale_sum) as amt , " +
      " sum(sale_price * sale_sum) / sum(sale_sum) as avg, sum(sale_price * sale_sum) / count(distinct order_no), " +
      " count(member_id) / count(distinct member_id),  sum(sale_price * sale_sum) / count(distinct member_id), count(distinct member_id)  " +
      " from order  group by district, cate_sid  order by amt desc")

//    categoryConsumerSum.map(s => s.getString(1))
//    categoryConsumerSum.map(s => (s.getString(1), s.getDouble(2))).distinct.collect.sortBy(_._2).foreach(println)

    // 商品区域消费总量分析

    val goodsConsumerSum = spark.sql("select district, cate_sid, goods_code,  sum(sale_price * sale_sum) as sum , sum(sale_sum) as amt , " +
      "sum(sale_price * sale_sum) / sum(sale_sum) as avg, sum(sale_price * sale_sum) / count(distinct order_no), " +
      " count(member_id) / count(distinct member_id),  sum(sale_price * sale_sum) / count(distinct member_id), count(distinct member_id)  " +
      " from order  group by district, cate_sid, goods_code  order by amt desc")






















  }

}
