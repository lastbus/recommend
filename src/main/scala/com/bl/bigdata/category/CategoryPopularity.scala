package com.bl.bigdata.category

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by HJT20 on 2016/9/2.
 */
class CategoryPopularity {

  def pop() {

    val sparkConf = new SparkConf().setAppName("category_pop")
    val sc = new SparkContext(sparkConf)
    val url = "jdbc:mysql://10.201.129.74:3306/recommend_system?user=root&password=bl.com"
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val popDF = sqlContext.jdbc(url, "bl_category_performance_category_popularity")

    val dateFormat = new SimpleDateFormat("yyyy-MM")
    val cal = Calendar.getInstance()
    cal.add(Calendar.MONTH, -1)
    val curMonth = dateFormat.format(cal.getTime)

    val lstMonthRdd = popDF.rdd.filter(row=>row.getString(3).equals(curMonth))

    //lstMonthRdd.foreach(println)

    val bl_category_performance_conf = sqlContext.jdbc(url, "bl_category_performance_conf")
    val bl_category_performance_conf_detail = sqlContext.jdbc(url, "bl_category_performance_conf_detail")
    bl_category_performance_conf.registerTempTable("bl_category_performance_conf")
    bl_category_performance_conf_detail.registerTempTable("bl_category_performance_conf_detail")


    //配置参数
    val conf = sqlContext.sql("select c.components, cd.sub_comp,c.weight*cd.weight weight from bl_category_performance_conf c join bl_category_performance_conf_detail cd on c.components=cd.components")


    val pv_weight = conf.rdd.filter(row => row.getString(1).equals("popularity_pv")).first().getDouble(2)
    val uv_weight = conf.rdd.filter(row => row.getString(1).equals("popularity_uv")).first().getDouble(2)
    val cus_weight = conf.rdd.filter(row => row.getString(1).equals("popularity_customer")).first().getDouble(2)
//


    val bc_pv_weight = sc.broadcast(pv_weight)
    val bc_uv_weight = sc.broadcast(uv_weight)
    val bc_cus_weight = sc.broadcast(cus_weight)
//
//
    val popScoreRdd = lstMonthRdd.map(row=>{
      val category_sid = row.getInt(0)
      val category_name = row.getString(1)
      val level =row.getInt(2)
      val month = row.getString(3)
      val pv = row.getInt(4)
      val uv = row.getInt(5)
      val number_of_customers = row.getInt(6)
      val pv_ratio = row.getDouble(7)
      val uv_ratio = row.getDouble(8)
      val customers_ratio = row.getDouble(9)
      val norm_pv_ratio = row.getDouble(10)
      val norm_uv_ratio = row.getDouble(11)
      val norm_customers_ratio = row.getDouble(12)
      val pv_score = norm_pv_ratio * bc_pv_weight.value
      val uv_score = norm_uv_ratio * bc_uv_weight.value
      val customer_score = norm_customers_ratio * bc_cus_weight.value
      val pop_score = pv_score + uv_score + customer_score

      Row(category_sid,category_name,level,month,pv,uv,number_of_customers,pv_ratio,uv_ratio,customers_ratio,
        norm_pv_ratio,norm_uv_ratio,norm_customers_ratio,pv_score,uv_score,customer_score,pop_score
      )
    })

    popScoreRdd.foreach(println)
    val popSchema = StructType(
      StructField("category_sid", IntegerType)
        :: StructField("category_name", StringType)
      :: StructField("level", org.apache.spark.sql.types.IntegerType)
      :: StructField("month", org.apache.spark.sql.types.StringType)
      :: StructField("pv", org.apache.spark.sql.types.IntegerType)
      :: StructField("uv", org.apache.spark.sql.types.IntegerType)
      :: StructField("number_of_customers", org.apache.spark.sql.types.IntegerType)
      :: StructField("pv_ratio", org.apache.spark.sql.types.DoubleType)
      :: StructField("uv_ratio", org.apache.spark.sql.types.DoubleType)
      :: StructField("customers_ratio", org.apache.spark.sql.types.DoubleType)
      :: StructField("norm_pv_ratio", org.apache.spark.sql.types.DoubleType)
      :: StructField("norm_uv_ratio", org.apache.spark.sql.types.DoubleType)
      :: StructField("norm_customers_ratio", org.apache.spark.sql.types.DoubleType)
      :: StructField("pv_score", org.apache.spark.sql.types.DoubleType)
      :: StructField("uv_score", org.apache.spark.sql.types.DoubleType)
      :: StructField("customer_score", org.apache.spark.sql.types.DoubleType)
      :: StructField("pop_score", org.apache.spark.sql.types.DoubleType)
      :: Nil)
     sqlContext.createDataFrame(popScoreRdd,popSchema).insertIntoJDBC(url,"bl_category_performance_category_popularity_score",true)
  }
}

object CategoryPopularity {

  def main(args: Array[String]) {

    val cp = new CategoryPopularity
    cp.pop()
  }
}
