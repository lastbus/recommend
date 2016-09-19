package com.bl.bigdata.category

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.util.SparkFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import java.sql.{DriverManager, PreparedStatement, Connection}

/**
 * Created by HJT20 on 2016/8/24.
 */
class CategorySalesComp {

  def categerySalesComp(day: Int): Unit = {
    val sc = SparkFactory.getSparkContext("CategorySalesComp")
    val hiveContext = new HiveContext(sc)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    var start = sdf.format(new Date(date0.getTime - 24000L * 3600 * day))

    val cateSql = "SELECT DISTINCT category_id,level1_id,CASE  WHEN level2_id IS NULL THEN 0 else level2_id END AS level2_id,CASE  WHEN level3_id IS NULL THEN 0 else level3_id END AS level3_id ,CASE  WHEN level4_id IS NULL THEN 0 else level4_id END AS level4_id,CASE  WHEN level5_id IS NULL THEN 0 else level5_id END AS level5_id FROM idmdata.dim_management_category"
    val cateRdd = hiveContext.sql(cateSql).rdd.map(row => (row.getLong(0), Seq((row.getLong(1), 1), (row.getLong(2), 2), (row.getLong(3), 3), (row.getLong(4), 4), (row.getLong(5), 5))))


    val saleSql = s"SELECT goods_sid,sale_price,sale_sum,item_amount,category_id,category_name,dt FROM recommendation.order_info where dt>$start and " +
      " ORDER_STATUS NOT IN ('1001', '1029', '1100') and category_id is not null "
    val salesRdd = hiveContext.sql(saleSql).rdd.map(row => (row.getLong(4), (row.getString(0), row.getDouble(1), row.getDouble(2),
      row.getDouble(3), row.getString(5), row.getString(6))))

    val totalSales = salesRdd.map { case ((category_id, (goods_sid, sale_price, sale_sum, item_amount, category_name, dt))) =>
      sale_price * sale_sum
    }.reduce((x, y) => x + y)

    val start90 = sdf.format(new Date(date0.getTime - 24000L * 3600 * 90))
    val sql90day = s"SELECT goods_sid,sale_price,sale_sum,item_amount,category_id,category_name,dt FROM recommendation.order_info where dt>$start90 and " +
      " ORDER_STATUS NOT IN ('1001', '1029', '1100') and category_id is not null"
    val sales90Rdd = hiveContext.sql(sql90day).rdd.map(row => (row.getLong(4), (row.getString(0), row.getDouble(1), row.getDouble(2),
      row.getDouble(3), row.getString(5), row.getString(6)))).mapValues(x => x._2 * x._3).reduceByKey(_ + _)

    val start180 = sdf.format(new Date(date0.getTime - 24000L * 3600 * 180))

    val sql180day = s"SELECT goods_sid,sale_price,sale_sum,item_amount,category_id,category_name,dt FROM recommendation.order_info where dt>$start180 and " +
      " ORDER_STATUS NOT IN ('1001', '1029', '1100') and category_id is not null"
    val sales180Rdd = hiveContext.sql(sql180day).rdd.map(row => (row.getLong(4), (row.getString(0), row.getDouble(1), row.getDouble(2),
      row.getDouble(3), row.getString(5), row.getString(6)))).mapValues(x => x._2 * x._3).reduceByKey(_ + _)

    //末级类目90 180销量
    val btmCateSale918Rdd = sales180Rdd.leftOuterJoin(sales90Rdd).mapValues { case (s18, s9) => {

      if (s9.isEmpty) {

        (s18, 0.0)
      }
      else {
        (s18, s9.get)
      }
    }
    }

    val CateSale918Rdd = btmCateSale918Rdd.join(cateRdd).map(cg => {
      val category_id = cg._1
      val sale918 = cg._2._1
      val cates = cg._2._2
      cates.filter(x => x._1 != 0).map(x => (x, sale918))
    }).flatMap(x => x).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues { case (s18, s9) => {
        if (s18 > 0) {
          (s18, s9, (s9 / 3 - s18 / 6) / (s18 / 6))
        }
        else {
          (s18, s9, 0.0)
        }

      }
      }
   // CateSale918Rdd.saveAsTextFile("/home/hdfs/CateSale918Rdd")

    val growthRdd = CateSale918Rdd.map(x => x._2._3)
    val bminGrow = sc.broadcast(growthRdd.min())
    val bmaxGrow = sc.broadcast(growthRdd.max())
    val normCateSaleGrowth = CateSale918Rdd.mapValues(x => {
      val ming = bminGrow.value
      val maxg = bmaxGrow.value
      if (ming != maxg && maxg > 0) {
        (x._1, x._2, x._3, (x._3 - ming) / (maxg - ming), ming, maxg)
      }
      else {
        (x._1, x._2, x._3, 0.0, ming, maxg)
      }

    }).map { case (((cate, level), (s18, s9, rate, norm_rate, ming, maxg))) =>
      (level, (cate, s18, s9, rate, norm_rate, ming, maxg))

    }.map { case ((level, gv)) => {
      ((level, gv._1), gv)
    }
    }




    val goodsSql = "SELECT DISTINCT sid,goods_sales_name,CASE WHEN category_id IS NULL THEN 0 else category_id END AS category_id ,category_name,yun_type,sale_status,stock FROM recommendation.goods_avaialbe_for_sale_channel WHERE sale_status =4 AND category_id IS NOT NULL"
    //sid,category_id
    val goodsRdd = hiveContext.sql(goodsSql).rdd.map(row => (row.getString(0), row.getLong(2)))

    //全部商品
    val totalGoodsNum = goodsRdd.map { case (g, c) => g }.count()
    val bcastTotalNum = sc.broadcast(totalGoodsNum)

    val bcastTotalSales = sc.broadcast(totalSales)

    val aveSingleGoodsSales = bcastTotalSales.value / bcastTotalNum.value

    val bcaveSingleGoodsSales = sc.broadcast(aveSingleGoodsSales)

    //末级品类商品
    val bottomCateGoodsRdd = goodsRdd.map(x => (x._2, Seq(x._1))).reduceByKey(_ ++ _).mapValues(x => x.distinct.size)
    //品类商品数Rdd
    val cateGoodsRdd = bottomCateGoodsRdd.join(cateRdd).map(cg => {
      val category_id = cg._1
      val goodsNum = cg._2._1
      val cates = cg._2._2
      cates.filter(x => x._1 != 0).map(x => (x, goodsNum))
    }).flatMap(x => x).reduceByKey(_ + _)


    val catesSalesRdd = salesRdd.join(cateRdd).map(cs => {
      val category_id = cs._1
      val sales = cs._2._1
      val cates = cs._2._2
      cates.filter(x => x._1 != 0).map(x => (x, sales))
    }).flatMap(x => x).mapValues { case (goods_sid, sale_price, sale_sum, item_amount, category_name, dt) =>
      sale_sum * sale_price
    }.reduceByKey(_ + _)
      .mapValues(x =>
        if (bcastTotalSales.value != 0) {
          (x, x / bcastTotalSales.value)
        } else {
          (x, 0.0)
        }
      )



    val maxRdd = catesSalesRdd.map { case (((cate, level), (s, sr))) => {
      (level, sr)
    }
    }.reduceByKey(_.max(_))

    val minRdd = catesSalesRdd.map { case (((cate, level), (s, sr))) => {
      (level, sr)
    }
    }.reduceByKey(_.min(_))


    //销量 商品数 Rdd
    val cateSaleGoodsNumRdd = cateGoodsRdd.leftOuterJoin(catesSalesRdd)
      .map { case (((cate, level), (goodsNum, s_sr))) => {
        if (s_sr.isEmpty) {
          (level, (cate, goodsNum, 0.0, 0.0))
        }
        else {
          (level, (cate, goodsNum, s_sr.get._1, s_sr.get._2))
        }
      }
      }.leftOuterJoin(minRdd).leftOuterJoin(maxRdd).mapValues { case ((((cate, goodsNum, sales, sales_ratio), smin), smax)) =>
      if (!smin.isEmpty && !smax.isEmpty && goodsNum != 0) {
        val minv = smin.get
        val maxv = smax.get

        if (minv != maxv) {

          (cate, sales, sales_ratio, (sales_ratio - minv) / (maxv - minv), goodsNum, sales / goodsNum, (sales / goodsNum) / bcaveSingleGoodsSales.value, bcaveSingleGoodsSales.value, bcastTotalNum.value, bcastTotalSales.value)
        }
        else {
          (cate, sales, sales_ratio, 0.0, goodsNum, sales / goodsNum, (sales / goodsNum) / bcaveSingleGoodsSales.value, bcaveSingleGoodsSales.value, bcastTotalNum.value, bcastTotalSales.value)
        }

      }
      else {
        (cate, sales, sales_ratio, 0.0, goodsNum, 0.0, (sales / goodsNum) / bcaveSingleGoodsSales.value, bcaveSingleGoodsSales.value, bcastTotalNum.value, bcastTotalSales.value)
      }

    }
    val singleSaleRateRdd = cateSaleGoodsNumRdd.map(x => x._2._7)
    val bmax_singleSaleRate = sc.broadcast(singleSaleRateRdd.max())
    val bmin_singleSaleRate = sc.broadcast(singleSaleRateRdd.min())

    val normCateSaleGoodsNumRdd = cateSaleGoodsNumRdd.mapValues {
      case ((cate, sales, sales_ratio, norm_sales_ratio, goodsNum, cate_single_sales, cate_single_sales_ratio, singleGoodsSale, totalNum, totalSales)) => {
        if (bmax_singleSaleRate.value != bmin_singleSaleRate.value) {
          (cate, sales, sales_ratio, norm_sales_ratio, goodsNum, cate_single_sales, cate_single_sales_ratio, (cate_single_sales_ratio - bmin_singleSaleRate.value) / (bmax_singleSaleRate.value - bmin_singleSaleRate.value), singleGoodsSale, totalNum, totalSales)
        }
        else {
          (cate, sales, sales_ratio, norm_sales_ratio, goodsNum, cate_single_sales, cate_single_sales_ratio, 0.0, singleGoodsSale, totalNum, totalSales)
        }
      }
    }

    //读取权重配置
    val url = "jdbc:mysql://10.201.129.74:3306/recommend_system?user=root&password=bl.com"
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val bl_category_performance_conf = sqlContext.jdbc(url, "bl_category_performance_conf")
    val bl_category_performance_conf_detail = sqlContext.jdbc(url, "bl_category_performance_conf_detail")
    bl_category_performance_conf.registerTempTable("bl_category_performance_conf")
    bl_category_performance_conf_detail.registerTempTable("bl_category_performance_conf_detail")

    val conf = sqlContext.sql("select c.components, cd.sub_comp,c.weight*cd.weight weight from bl_category_performance_conf c join bl_category_performance_conf_detail cd on c.components=cd.components")
    val nds_weight = conf.rdd.filter(row => row.getString(1).equals("ninty_day_sales")).first().getDouble(2)
    val sgs_weight = conf.rdd.filter(row => row.getString(1).equals("single_good_sale")).first().getDouble(2)
    val sgr_weight = conf.rdd.filter(row => row.getString(1).equals("sales_grow_rate")).first().getDouble(2)

    val bc_nds_weight = sc.broadcast(nds_weight)
    val bc_sgs_weight = sc.broadcast(sgs_weight)
    val bc_sgr_weight = sc.broadcast(sgr_weight)

    val outRdd = normCateSaleGoodsNumRdd.map { case (lvl, vs) =>
      ((lvl, vs._1), vs)
    }.leftOuterJoin(normCateSaleGrowth)
      .mapValues { case (sale, growth) => {
        if (!growth.isEmpty) {
          (sale, growth.get)

        }
        else {
                 //cate,s18,s9,rate,norm_rate
          (sale, (0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
        }
      }

      }


    var conn: Connection = null
    var ps: PreparedStatement = null
    val msql = "insert into bl_category_performance_category_sale_score(category_sid, level," +
      " ninety_day_sales,ninety_day_sales_ratio,norm_ninety_day_sales_ratio,ninety_day_sales_score,single_sku_sales," +
      "single_sku_sales_ratio, norm_single_sku_sales_ratio,single_sku_sales_score,ave_growth_rate,norm_ave_growth_rate,ave_growth_rate_score,sales_score,cdate)" +
      " values (?, ?,?,?,?,?,?,?,?,?,?,?,?,?,now())"
    try {
      outRdd.foreachPartition(partition => {
        val driver = "com.mysql.jdbc.Driver"
        Class.forName(driver)
        conn = DriverManager.getConnection("jdbc:mysql://10.201.129.74:3306/recommend_system", "root", "bl.com")
        partition.foreach { data => {
          ps = conn.prepareStatement(msql)
          ps.setLong(1, data._1._2)
          ps.setInt(2, data._1._1)
          //90天销量
          ps.setDouble(3, data._2._1._2)
          //90销量占比
          ps.setDouble(4, data._2._1._3)
          //归一化
          ps.setDouble(5, data._2._1._4)
          //得分
          ps.setDouble(6, data._2._1._4 * bc_nds_weight.value)

          //单sku平均销售
          ps.setDouble(7, data._2._1._6)
          //单sku销售比
          ps.setDouble(8, data._2._1._7)
          //归一化
          ps.setDouble(9, data._2._1._8)
          //得分
          ps.setDouble(10, data._2._1._8 * bc_sgs_weight.value)
          //增长率
          // /cate,s18,s9,rate,norm_rate
          ps.setDouble(11, data._2._2._4)

          ps.setDouble(12, data._2._2._5)
          ps.setDouble(13, data._2._2._5 * bc_sgr_weight.value)
          ps.setDouble(14, data._2._1._8 * bc_sgs_weight.value + data._2._1._4 * bc_nds_weight.value + data._2._2._5 * bc_sgr_weight.value)

          ps.executeUpdate()
        }
        }
      }
      )
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
   sc.stop()
  }


}

object CategorySalesComp {
  def main(args: Array[String]): Unit = {
    val cate = new CategorySalesComp
    cate.categerySalesComp(90)
  }
}
