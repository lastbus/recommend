package com.bl.bigdata.category

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.bl.bigdata.util.SparkFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/9/2.
 */
class CategoryOperation {

  def categoryOperation(day: Int): Unit = {

    val sparkConf = new SparkConf().setAppName("category_pop")
    val sc = new SparkContext(sparkConf)
    val url = "jdbc:mysql://10.201.129.74:3306/recommend_system?user=root&password=bl.com"
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)



    val operRdd = sqlContext.jdbc(url, "bl_category_performance_category_operation")


    //lstMonthRdd.foreach(println)

    val bl_category_performance_conf = sqlContext.jdbc(url, "bl_category_performance_conf")
    val bl_category_performance_conf_detail = sqlContext.jdbc(url, "bl_category_performance_conf_detail")
    bl_category_performance_conf.registerTempTable("bl_category_performance_conf")
    bl_category_performance_conf_detail.registerTempTable("bl_category_performance_conf_detail")


    //配置参数
    val conf = sqlContext.sql("select c.components, cd.sub_comp,c.weight*cd.weight weight from bl_category_performance_conf c join bl_category_performance_conf_detail cd on c.components=cd.components")
    val stock_weight = conf.rdd.filter(row => row.getString(1).equals("operation_stock")).first().getDouble(2)
    val prop_weight = conf.rdd.filter(row => row.getString(1).equals("operation_prop")).first().getDouble(2)
    val price_weight = conf.rdd.filter(row => row.getString(1).equals("operation_price")).first().getDouble(2)
    val shelf_weight = conf.rdd.filter(row => row.getString(1).equals("operation_shelf")).first().getDouble(2)
    val ssr_weight = conf.rdd.filter(row => row.getString(1).equals("operation_ssr")).first().getDouble(2)

    //
    val bc_stock_weight = sc.broadcast(stock_weight)
    val bc_prop_weight = sc.broadcast(prop_weight)
    val bc_price_weight = sc.broadcast(price_weight)
    val bc_shelf_weight = sc.broadcast(shelf_weight)
    val bc_ssr_weight = sc.broadcast(ssr_weight)


    val minMaxRdd = operRdd.map(row=>{
      val level = row.getInt(2)

      var stock_sku_rate = 0.0
      if( !row.get(3).isInstanceOf[Double])
      {
        stock_sku_rate = 0.0
      }
      else
      {
        stock_sku_rate = row.getDouble(3)
      }

      var ave_prop_fill_rate = 0.0
      if( !row.get(4).isInstanceOf[Double])
      {
        ave_prop_fill_rate = 0.0
      }
      else
      {
        ave_prop_fill_rate = row.getDouble(4)
      }

      var shelf_sale_ratio = 0.0
      if( !row.get(7).isInstanceOf[Double])
      {
        shelf_sale_ratio = 0.0
      }
      else
      {
        shelf_sale_ratio = row.getDouble(7)
      }


      var ave_price_adjustment_time = 0.0
      if( !row.get(5).isInstanceOf[Double])
      {
        ave_price_adjustment_time = 0.0
      }
      else
      {
        ave_price_adjustment_time = row.getDouble(5)
      }

      var ave_on_off_shelf_time = 0.0
      if( !row.get(6).isInstanceOf[Double])
      {
        ave_on_off_shelf_time = 0.0
      }
      else
      {
        ave_on_off_shelf_time = row.getDouble(6)
      }
      (level,Seq((ave_price_adjustment_time,ave_on_off_shelf_time,stock_sku_rate,ave_prop_fill_rate,shelf_sale_ratio)))
    }).reduceByKey(_ ++ _).mapValues(x=>((x.maxBy(_._1)._1,x.minBy(_._1)._1),(x.maxBy(_._2)._2,x.minBy(_._2)._2)
      ,(x.maxBy(_._3)._3,x.minBy(_._3)._3),(x.maxBy(_._4)._4,x.minBy(_._4)._4),(x.maxBy(_._5)._5,x.minBy(_._5)._5)))


    val rawRdd = operRdd.map(row=>{
      val category_sid = row.getInt(0)
      val category_name = row.getString(1)
      val level = row.getInt(2)
      var stock_sku_rate = 0.0
      if( !row.get(3).isInstanceOf[Double])
        {
          stock_sku_rate = 0.0
        }
      else
        {
          stock_sku_rate = row.getDouble(3)
        }

      var  ave_prop_fill_rate = 0.0
      if( !row.get(4).isInstanceOf[Double])
      {
        ave_prop_fill_rate = 0.0
      }
      else
      {
        ave_prop_fill_rate = row.getDouble(4)
      }

      var shelf_sale_ratio = 0.0
      if( !row.get(7).isInstanceOf[Double])
      {
        shelf_sale_ratio = 0.0
      }
      else
      {
        shelf_sale_ratio = row.getDouble(7)
      }
      var ave_price_adjustment_time = 0.0
      if( !row.get(5).isInstanceOf[Double])
      {
        ave_price_adjustment_time = 0.0
      }
      else
      {
        ave_price_adjustment_time = row.getDouble(5)
      }

      var ave_on_off_shelf_time = 0.0
      if( !row.get(6).isInstanceOf[Double])
      {
        ave_on_off_shelf_time = 0.0
      }
      else
      {
        ave_on_off_shelf_time = row.getDouble(6)
      }

      (level,(category_sid,category_name,level,stock_sku_rate,ave_prop_fill_rate,shelf_sale_ratio,ave_price_adjustment_time,ave_on_off_shelf_time))
    })

    val normRdd = rawRdd.join(minMaxRdd).map(x=> {
      val category_sid = x._2._1._1
      val category_name = x._2._1._2
      val level = x._2._1._3
      val stock_sku_rate = x._2._1._4
      val ave_prop_fill_rate = x._2._1._5
      val shelf_sale_ratio = x._2._1._6
      val ave_price_adjustment_time = x._2._1._7
      val ave_on_off_shelf_time = x._2._1._8
      val max_price = x._2._2._1._1
      val min_price = x._2._2._1._2
      val max_shelf = x._2._2._2._1
      val min_shelf = x._2._2._2._2
      var norm_price = 0.0
      if (max_price != min_price) {
        norm_price = (ave_price_adjustment_time - min_price) / (max_price - min_price)
      }

      var norm_shelf = 0.0
      if (max_shelf != min_shelf) {
        norm_shelf = (ave_on_off_shelf_time - min_shelf) / (max_shelf - min_shelf)
      }

      var norm_stock_sku_rate = 0.0
      val max_stock_rate = x._2._2._3._1
      val min_stock_rate = x._2._2._3._2
      if(max_stock_rate != min_stock_rate)
        {
          norm_stock_sku_rate = (stock_sku_rate - min_stock_rate)/(max_stock_rate - min_stock_rate)
        }

      val stock_sku_rate_score = norm_stock_sku_rate * bc_stock_weight.value


      var norm_prop_fill_rate = 0.0
      val max_prop_fill_rate = x._2._2._4._1
      val min_prop_fill_rate = x._2._2._4._2
      if(max_prop_fill_rate != min_prop_fill_rate)
      {
        norm_prop_fill_rate = (ave_prop_fill_rate - min_prop_fill_rate)/(max_prop_fill_rate - min_prop_fill_rate)
      }
      val prop_fill_rate_score = norm_prop_fill_rate * bc_prop_weight.value


      var norm_shelf_sale = 0.0
      val max_shelf_sale = x._2._2._5._1
      val min_shelf_sale = x._2._2._5._2
      if(max_shelf_sale != min_shelf_sale)
      {
        norm_shelf_sale = (shelf_sale_ratio - min_shelf_sale)/(max_shelf_sale - min_shelf_sale)
      }
      val shelf_sale_ratio_score = norm_shelf_sale * bc_ssr_weight.value


      val price_time_score = norm_price * bc_price_weight.value
      val shelf_time_score = norm_shelf * bc_shelf_weight.value
      val oper_score = stock_sku_rate_score + prop_fill_rate_score + shelf_sale_ratio_score + price_time_score + shelf_time_score
      Row(category_sid, category_name, level, stock_sku_rate, ave_prop_fill_rate, ave_price_adjustment_time,
        ave_on_off_shelf_time, shelf_sale_ratio, stock_sku_rate_score, prop_fill_rate_score, price_time_score,
        shelf_time_score, shelf_sale_ratio_score, oper_score)
    })

      val popSchema = StructType(
        StructField("category_sid", IntegerType)
          :: StructField("category_name", StringType)
          :: StructField("level", org.apache.spark.sql.types.IntegerType)
          :: StructField("stock_sku_rate", org.apache.spark.sql.types.DoubleType)
          :: StructField("ave_prop_fill_rate", org.apache.spark.sql.types.DoubleType)
          :: StructField("ave_price_adjustment_time", org.apache.spark.sql.types.DoubleType)
          :: StructField("ave_on_off_shelf_time", org.apache.spark.sql.types.DoubleType)
          :: StructField("shelf_sale_ratio", org.apache.spark.sql.types.DoubleType)
          :: StructField("out_of_stock_score", org.apache.spark.sql.types.DoubleType)
          :: StructField("prop_fill_score", org.apache.spark.sql.types.DoubleType)
          :: StructField("price_adj_score", org.apache.spark.sql.types.DoubleType)
          :: StructField("off_shelf_score", org.apache.spark.sql.types.DoubleType)
          :: StructField("stock_sale_ratio_score", org.apache.spark.sql.types.DoubleType)
          :: StructField("oper_score", org.apache.spark.sql.types.DoubleType)
          :: Nil)

    sqlContext.createDataFrame(normRdd,popSchema).insertIntoJDBC(url,"bl_category_performance_category_operation_score",true)
  }


}

object CategoryOperation {

  def main(args: Array[String]) {
    val co = new CategoryOperation
    co.categoryOperation(90)
  }

}
