package com.bl.bigdata.category

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.util.SparkFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

/**
 * Created by HJT20 on 2016/9/12.
 */
class CategoryBLGoodsPrice {

  def blGoodsPrice(): Unit ={
    val sc = SparkFactory.getSparkContext("category_match")
    val hiveContext = new HiveContext(sc)
    val cateSql = "SELECT DISTINCT category_id,level1_id,CASE  WHEN level2_id IS NULL THEN 0 else level2_id END AS level2_id,CASE  WHEN level3_id IS NULL THEN 0 else level3_id END AS level3_id ,CASE  WHEN level4_id IS NULL THEN 0 else level4_id END AS level4_id,CASE  WHEN level5_id IS NULL THEN 0 else level5_id END AS level5_id FROM idmdata.dim_management_category"
    val cateRdd = hiveContext.sql(cateSql).rdd.map(row => (row.getLong(0), Seq((row.getLong(1), 1), (row.getLong(2), 2), (row.getLong(3), 3), (row.getLong(4), 4), (row.getLong(5), 5))))
    val goodsSql = "SELECT DISTINCT sid,goods_sales_name,CASE WHEN category_id IS NULL THEN 0 else category_id END AS category_id ,category_name,sale_price,yun_type,sale_status,stock FROM recommendation.goods_avaialbe_for_sale_channel WHERE sale_status = 4 AND category_id IS NOT NULL"
    //sid,category_id
    val goodsRdd = hiveContext.sql(goodsSql).rdd.map(row => (row.getLong(2),(row.getString(0),row.getDouble(4))))

    val cateGoodsRdd = cateRdd.join(goodsRdd).flatMap(cg => {
      val category_id = cg._1
      val sale_price = cg._2._2._2
      val cates = cg._2._1
      cates.filter(x => x._1 != 0).map(x => (x, Seq(sale_price)))
    }).reduceByKey(_ ++ _).mapValues(x=>x.sorted).flatMapValues(x=>{
      val pmin = x(0)
      val pmax = x(x.size-1)
      val delt = (pmax - pmin) / 19.0
      x.map(p=>
      {
        val rNo = (Math.floor((p-pmin)/delt)).toInt
        val low = (pmin + rNo * delt).toInt
        val high = (pmin + (1 + rNo) * delt).toInt
        ((rNo,low,high),1)
      })
    }).map(x=>{
      val category_sid = x._1._1
      val level = x._1._2
      val rn = x._2._1._1
      val lp  = x._2._1._2
      val hp = x._2._1._3
      val gn = x._2._2
      (((category_sid,level),(rn,lp,hp)),gn)
    }).reduceByKey(_ + _)


    val cateDistRdd = cateGoodsRdd.map(x=>
      Row(x._1._1._1.toInt,x._1._2._1,x._1._2._2.toDouble,x._1._2._3.toDouble,0.toInt,x._2.toInt)
    )
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * 60))
    val saleSql = s"SELECT goods_sid,sale_price,sale_sum,item_amount,category_id,category_name,dt FROM recommendation.order_info where dt>$start and ORDER_STATUS NOT IN ('1001', '1029', '1100') and category_id is not null and order_type_code = 1"
    val salesRdd = hiveContext.sql(saleSql).rdd.map(row => (row.getLong(4), (row.getDouble(1), row.getDouble(2))))
    val cateSaleRdd = cateRdd.join(salesRdd).flatMap(cg => {
      val category_id = cg._1
      val sale_price = cg._2._2._1
      val sale_sum = cg._2._2._2
      val sale_monye = sale_price * sale_sum
      val cates = cg._2._1
      cates.filter(x => x._1 != 0).map(x => (x, (sale_price,sale_sum,sale_monye)))
    })

    val salePriceRdd = cateGoodsRdd.map(x=>{
      val cl = x._1._1
      val pr = x._1._2
      (cl,pr)
    }).leftOuterJoin(cateSaleRdd).mapValues(x=>{
     val rlh = x._1
      val rn = rlh._1
      val lp = rlh._2
      val hp = rlh._3
      val sp = x._2
      var price = 0.0
      var sale_sum = 0.0
      var sale_monye = 0.0

      if(!sp.isEmpty)
        {
          price = sp.get._1
          sale_sum = sp.get._2
          sale_monye = sp.get._3
          if(price>=lp && price<hp)
            {
              (rn,lp,hp,sale_sum)
            }
          else
            {
              (rn,lp,hp,0.0)
            }
        }
      else
        {
          (rn,lp,hp,sale_sum)
        }

    }).map(x=>{
      val cl = x._1
      val pr =  x._2
      val rn = pr._1
      val lp = pr._2
      val hp = pr._3
      val sm = pr._4
      (((cl._1,cl._2),(rn,lp,hp)),sm)
    }).reduceByKey(_ + _)


    val saleOutRdd = salePriceRdd.map(x=>

      Row(x._1._1._1.toInt,x._1._2._1,x._1._2._2.toDouble,x._1._2._3.toDouble,1.toInt,x._2.toInt)
    )
    val outRdd = cateDistRdd.union(saleOutRdd)

    val priceSchema = StructType(
      StructField("category_sid", IntegerType)
        :: StructField("range_no", IntegerType)
        :: StructField("low_price", org.apache.spark.sql.types.DoubleType)
        :: StructField("high_price", org.apache.spark.sql.types.DoubleType)
        :: StructField("type", org.apache.spark.sql.types.IntegerType)
        :: StructField("sale_sum", org.apache.spark.sql.types.IntegerType)
        :: Nil)
    val url = "jdbc:mysql://10.201.129.74:3306/recommend_system?user=root&password=bl.com"
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.createDataFrame(outRdd,priceSchema).insertIntoJDBC(url,"bl_category_performance_category_price_conf",true)

  }
}

object CategoryBLGoodsPrice {
  def main(args: Array[String]) {

  }

}
