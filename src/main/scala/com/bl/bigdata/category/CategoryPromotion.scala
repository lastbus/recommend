package com.bl.bigdata.category

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.util.SparkFactory
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/9/7.
 */
class CategoryPromotion {

  def cateProm(): Unit = {
    val sc = SparkFactory.getSparkContext("category_match")
    val hiveContext = new HiveContext(sc)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val sdf2 = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val act_start_time = sdf.format(new Date(date0.getTime - 24000L * 3600 * 60))
    val dt = sdf2.format(new Date(date0.getTime - 24000L * 3600 * 1))

    val pgSql1 = s"SELECT sale_time,act_start_time,act_end_time,sku_id,sku_name,brand_sid,cn_name,management_category_id,management_category_name ,sale_price,sale_sum,sale_money_sum,need_money FROM idmdata.m_da_globle_promo_goods where dt='$dt'  and act_start_time>'$act_start_time'"
    val pgSql2 = s"SELECT DISTINCT sku_id,management_category_id,enable_time_from,enable_time_to FROM idmdata.m_da_globle_coupon_goods where management_category_id is not null   and enable_time_from>'$act_start_time'"


    val promgdDf1 = hiveContext.sql(pgSql1)
    val coupongdDf = hiveContext.sql(pgSql2)

    promgdDf1.registerTempTable("prom_goods")

    val goodsDf = hiveContext.sql("select distinct sku_id,management_category_id,act_start_time,act_end_time from prom_goods where management_category_id is not null")

    val gcDf = goodsDf.unionAll(coupongdDf)

    //参与频次
    val goodsRdd = gcDf.map(row=>{
      (row.getLong(1),(row.getString(0),row.getString(2),row.getString(3)))
    })


    val  avblGoodsDf = hiveContext.sql("SELECT sid,category_id FROM recommendation.goods_avaialbe_for_sale_channel WHERE sale_status=4 and category_id is not null")

    val avblGoodsRdd = avblGoodsDf.map(row=>{
      (row.getLong(1),row.getString(0))
    })

    val cateSql = "SELECT DISTINCT category_id,level1_id,CASE  WHEN level2_id IS NULL THEN 0 else level2_id END AS level2_id,CASE  WHEN level3_id IS NULL THEN 0 else level3_id END AS level3_id ,CASE  WHEN level4_id IS NULL THEN 0 else level4_id END AS level4_id,CASE  WHEN level5_id IS NULL THEN 0 else level5_id END AS level5_id FROM idmdata.dim_management_category"
    val cateRdd = hiveContext.sql(cateSql).rdd.map(row => (row.getLong(0), Seq((row.getLong(1), 1), (row.getLong(2), 2), (row.getLong(3), 3), (row.getLong(4), 4), (row.getLong(5), 5))))

    val catesPromGoodsRdd = goodsRdd.join(cateRdd).map(cg => {
      val category_id = cg._1
      val goodsId = cg._2._1
      val cates = cg._2._2
      cates.filter(x => x._1 != 0).map(x => (x, goodsId))
    }).flatMap(x=>x).aggregateByKey((Set[String](), 0))((r: (Set[String], Int), item: (String, String, String)) => (r._1 + item._1, r._2 + 1),
      (a: (Set[String], Int), b:(Set[String], Int)) => (a._1 ++ b._1, a._2 + b._2)).mapValues(s => (s._1.size, s._2)) //.mapValues(x=>Seq(x)).reduceByKey(_ ++ _).mapValues(x=>x.size)



    val cateAvblGoodsRdd = avblGoodsRdd.join(cateRdd).map(cg => {
      val category_id = cg._1
      val goodsId = cg._2._1
      val cates = cg._2._2
      cates.filter(x => x._1 != 0).map(x => (x, goodsId))
    }).flatMap(x=>x).mapValues(x=>Seq(x)).reduceByKey(_ ++ _).mapValues(x=>x.size)


    val pgfRdd = catesPromGoodsRdd.join(cateAvblGoodsRdd).mapValues{case(pg,ag) =>{
      (pg._1,pg._2,ag,pg._1.toDouble/ag,pg._2.toDouble/ag)
    }}

    val mxn1Rdd = pgfRdd.map(x=>x._2._4)
    val min1 = mxn1Rdd.min()
    val max1 = mxn1Rdd.max()

    val mxn2Rdd = pgfRdd.map(x=>x._2._5)

    val min2 = mxn2Rdd.min()
    val max2 = mxn2Rdd.max()

    val bc_min1 = sc.broadcast(min1)
    val bc_max1 = sc.broadcast(max1)
    val bc_min2 = sc.broadcast(min2)
    val bc_max2 = sc.broadcast(max2)

    val normpgfRdd = pgfRdd.mapValues(x=>{
      var norm1 = 0.0
      var norm2 = 0.0
      val _min1 = bc_min1.value
      val _max1 = bc_max1.value

      if(_min1 == _max1)
        norm1 = 0.0
      else
        norm1 = (x._4 - _min1)/(_max1 - _min1)

      val _min2 = bc_min2.value
      val _max2 = bc_max2.value
      if(_min2 == _max2)
        norm2 = 0.0
      else
        norm2 = (x._5 - _min2)/(_max2 - _min2)
      (x._1,x._2,x._3,x._4,x._5,norm1,norm2)

    })


  }
}

object CategoryPromotion {

  def main(args: Array[String]) {

  }
}
