package com.bl.bigdata.product

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.bl.bigdata.spider.MysqlConnection
import com.bl.bigdata.util.SparkFactory
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

/**
  * Created by MK33 on 2016/8/18.
  */
object CategoryStatistic {

  def main(args: Array[String]) {
    val hiveContext = SparkFactory.getHiveContext


//    bl_category_performance_basic(hiveContext)
//    bl_category_performance_category_brand(hiveContext)
//    bl_category_performance_category_monthly_sales(hiveContext)
//    bl_category_performance_category_monthly_hotcakes(hiveContext)
//    bl_category_performance_category_operation(hiveContext)
    bl_category_performance_monthly_goods_shelf_sales_ratio(hiveContext)
//    bl_category_performance_category_popularity(hiveContext)


  }




  def bl_category_performance_category_popularity(hiveContext: HiveContext): Unit = {

    val tableMysql = "bl_category_performance_category_popularity"
    val categorySql = "  SELECT c.category_id, c.level1_id, c.level1_name, c.level2_id, c.level2_name, " +
      "   c.level3_id, c.level3_name,  c.level4_id, c.level4_name, c.level5_id, c.level5_name " +
      "   FROM idmdata.dim_management_category c "

    val categoryRawRDD = hiveContext.sql(categorySql).map { r =>
      ( if (r.isNullAt(0)) -1 else r.getLong(0),
        (if (r.isNullAt(1)) -1 else r.getLong(1), r.getString(2),
          if (r.isNullAt(3)) -1 else r.getLong(3), r.getString(4),
          if (r.isNullAt(5)) -1 else r.getLong(5), r.getString(6),
          if (r.isNullAt(7)) -1 else r.getLong(7), r.getString(8),
          if (r.isNullAt(9)) -1 else r.getLong(9), r.getString(10)) )
    }.distinct().partitionBy(new HashPartitioner(10)).cache()

    val userBehaviorSql = " SELECT u.category_sid, count(u.cookie_id) as pv, count(DISTINCT u.cookie_id) as uv, substring(u.event_date, 0, 7) as month, " +
                          " sum( CASE when u.behavior_type = '4000' THEN 1 ELSE 0 END) as customers " +
                          " FROM recommendation.user_behavior_raw_data u " +
                          " WHERE u.behavior_type = '4000' OR u.behavior_type = '1000' GROUP BY u.category_sid, substring(u.event_date, 0, 7) "

    val a  = hiveContext.sql(userBehaviorSql)
     a.registerTempTable("tmp")
    val monthData = hiveContext.sql("select month, sum(pv), sum(uv), sum(customers) from tmp group by month ").map { r =>
      (r.getString(0), (if (r.isNullAt(1)) 0 else r.getLong(1), if (r.isNullAt(2)) 0 else r.getLong(2), if (r.isNullAt(3)) 0 else r.getLong(3)))
    }.collect().toMap

    val b = a.map { r =>
      (if (r.isNullAt(0) || r.get(0).toString.equalsIgnoreCase("null")) -99L else r.getString(0).toLong,
        (if (r.isNullAt(1)) 0 else r.getLong(1), if (r.isNullAt(2)) 0 else r.getLong(2), r.getString(3), if (r.isNullAt(4)) 0 else r.getLong(4)))
    }

    val c = categoryRawRDD.join(b).map { case (cate, ((lev1, lev1Name, lev2, lev2Name, lev3, lev3Name, lev4, lev4Name, lev5, lev5Name), (pv, uv, month, customs))) =>
        Array((lev1, lev1Name, 1), (lev2, lev2Name, 2), (lev3, lev3Name, 3), (lev4, lev4Name, 4), (lev5, lev5Name, 5)).filter(_._1 != -1)
          .map(s=> ((s._1, s._2, s._3, month), (pv, uv, customs)))
    }.flatMap(s =>s).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)).map { case ((cate, cateName, lev, month), (pv, uv, customs)) =>
      val data = monthData.getOrElse(month, (1L, 1L, 1L))
      (cate, cateName, lev, month, pv, uv, customs, pv.toDouble / data._1, uv.toDouble / data._2, customs.toDouble / data._3)
    }
    c.cache()
    val pvUvCustomBrd = hiveContext.sparkContext.broadcast(c.map(s => ((s._3, s._4), (s._5, s._6, s._7))).
      aggregateByKey((Long.MaxValue, 0L, Long.MaxValue, 0L, Long.MaxValue, 0L))((a: (Long, Long, Long, Long, Long, Long), b: (Long, Long, Long)) =>
        (Math.min(a._1, b._1), Math.max(a._2, b._1), Math.min(a._3, b._2), Math.max(a._4, b._2), Math.min(a._5, b._3), Math.max(a._6, b._3)),
        (a: (Long, Long, Long, Long, Long, Long), b: (Long, Long, Long, Long, Long, Long)) =>
          (Math.min(a._1, b._1), Math.max(a._2, b._2), Math.min(a._3, b._3), Math.max(a._4, b._4), Math.min(a._5, b._5), Math.max(a._6, b._6))).collectAsMap())


    val insertIntoMysql = s"replace into $tableMysql (category_sid, category_name, level, month, pv, uv, number_of_customers, pv_ratio, uv_ratio, customers_ratio, norm_pv_ratio, norm_uv_ratio, norm_cust_ratio, cdate) " +
                          s"values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now()) "

    c.foreachPartition { partition =>
      val conn = MysqlConnection.connection
      conn.setAutoCommit(false)
      val pt = conn.prepareStatement(insertIntoMysql)
      val n = 2000
      var i = 0
      val pvUvCustomValue = pvUvCustomBrd.value
      partition.foreach { case (cate, cateName, lev, month, pv, uv, custm, pvR, uvR, custmR) =>
          pt.setInt(1, cate.toInt)
          pt.setString(2, cateName)
          pt.setInt(3, lev)
          pt.setString(4, month)
          pt.setInt(5, pv.toInt)
          pt.setInt(6, uv.toInt)
          pt.setInt(7, 0)
          pt.setDouble(8, pvR)
          pt.setDouble(9, uvR)
          pt.setDouble(10, custmR)
          val minMax = pvUvCustomValue((lev, month))
          pt.setDouble(11, if ((minMax._2 - minMax._1) == 0.0) 0.0 else (pv - minMax._1).toDouble / (minMax._2 - minMax._1))
          pt.setDouble(12, if ((minMax._4 - minMax._3) == 0.0 ) 0.0 else (uv - minMax._3).toDouble / (minMax._4 - minMax._3))
          pt.setDouble(13, if ((minMax._6 - minMax._5) == 0.0) 0.0 else (custm - minMax._5).toDouble / (minMax._6 - minMax._5))
          pt.addBatch()
          i += 1
          if (i % n == 0) {
            pt.executeBatch()
            conn.commit()
            Thread.sleep(200)
          }
      }
      pt.executeBatch()
      conn.commit()
    }

    val custmsSql = "select category_id, substring(sale_time, 0, 7), count(distinct member_id)  " +
      "  from recommendation.order_info  where ORDER_STATUS NOT IN ('1001', '1029', '1100')   group by category_id, substring(sale_time, 0, 7) "

    val update = "update recommend_system.bl_category_performance_category_popularity  set number_of_customers = ?  where category_sid = ? and month = ? "

    hiveContext.sql(custmsSql).map {
      r => (if (r.isNullAt(0) || r.get(0) == null || r.get(0).toString.equalsIgnoreCase("null")) -1 else r.getLong(0).toInt, r.getString(1), r.getLong(2).toInt)
    }.filter(_._1 != -1).foreachPartition{ partitition=>
        val conn = MysqlConnection.connection
        conn.setAutoCommit(false)
        val pt = conn.prepareStatement(update)
        val n = 2000
        var i = 0
        partitition.foreach { case (cate, month, customCount)  =>
            pt.setInt(1, customCount)
            pt.setInt(2, cate)
            pt.setString(3, month)
            pt.addBatch()
            i += 1
            if (i % n == 0) {
              pt.executeBatch()
              conn.commit()
              Thread.sleep(200)
          }
        }
        pt.executeBatch()
        conn.commit()


    }


  }



  def bl_category_performance_monthly_goods_shelf_sales_ratio(hiveContext: HiveContext): Unit = {
    val tableMysql = "bl_category_performance_monthly_goods_shelf_sales_ratio"

    val categorySql = "  SELECT c.category_id, c.level1_id, c.level1_name, c.level2_id, c.level2_name, " +
      "   c.level3_id, c.level3_name,  c.level4_id, c.level4_name, c.level5_id, c.level5_name " +
      "   FROM idmdata.dim_management_category c "

    val insertStockSql = s"replace into  $tableMysql   (category_sid, category_name, level, goods_sid, goods_sales_name, stock, cdate) values (?, ?, ?, ?, ?, ?, now()) "

    val categoryRawRDD = hiveContext.sql(categorySql).map { r =>
      ( if (r.isNullAt(0)) -1 else r.getLong(0),
        (if (r.isNullAt(1)) -1 else r.getLong(1), r.getString(2),
          if (r.isNullAt(3)) -1 else r.getLong(3), r.getString(4),
          if (r.isNullAt(5)) -1 else r.getLong(5), r.getString(6),
          if (r.isNullAt(7)) -1 else r.getLong(7), r.getString(8),
          if (r.isNullAt(9)) -1 else r.getLong(9), r.getString(10)) )
    }.distinct().partitionBy(new HashPartitioner(10)).cache()

    // stock
    val goodsForSaleSql = "SELECT g.sid, g.goods_sales_name, g.category_id, s.stock  FROM recommendation.goods_avaialbe_for_sale_channel g   " +
                          "JOIN recommendation.category_performance_goods_stock s ON s.sid = g.sid  "

    val goodsRawRDD = hiveContext.sql(goodsForSaleSql).map { r =>
      (if (r.isNullAt(2) || r.get(2) == null || r.get(2).toString.equalsIgnoreCase("null")) -99 else r.getLong(2),
        (r.getString(0), r.getString(1), if (r.isNullAt(3) || r.get(3).toString.equalsIgnoreCase("null")) 0 else r.getDouble(3).toInt))
    }.distinct()

    val tmp1 = categoryRawRDD.join(goodsRawRDD).map { case (cate, ((lev1, lev1Name, lev2, lev2Name, lev3, lev3Name, lev4, lev4Name, lev5, lev5Name), (goodsId, goodsName, stock))) =>
        Array((lev1, 1, lev1Name, goodsId, goodsName, stock),
          (lev2, 2, lev2Name, goodsId, goodsName, stock),
          (lev3, 3, lev3Name, goodsId, goodsName, stock),
          (lev4, 4, lev4Name, goodsId, goodsName, stock),
          (lev5, 5, lev5Name, goodsId, goodsName, stock)).filter(_._1 != -1)
    }.flatMap(s => s)

    tmp1.foreachPartition { partition =>
      val conn = MysqlConnection.connection
      conn.setAutoCommit(false)
      val pt = conn.prepareStatement(insertStockSql)
      val n = 200
      var i = 0
      partition.foreach { case (levId, lev, leveName, goodsId, goodsName, stock) =>
          pt.setInt(1, levId.toInt)
          pt.setString(2, leveName)
          pt.setInt(3, lev)
          pt.setInt(4, goodsId.toInt)
          pt.setString(5, goodsName)
          pt.setInt(6, stock)
          pt.addBatch()
          i += 1
          if (i % n == 0) {
            pt.executeBatch()
            conn.commit()
            Thread.sleep(200)
          }
      }
      pt.executeBatch()
      conn.commit()
    }

    // avg_day_sale_sum
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -30)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val dayBefore30 = sdf.format(cal.getTime)

    val avgSaleNumSql = " SELECT o.category_id, o.goods_sid, sum(o.sale_sum)  FROM recommendation.order_info o  " +
                        s" WHERE o.sale_time >= '$dayBefore30' AND o.order_type_code = '1' AND isnotnull(o.category_id)  GROUP BY o.category_id, o.goods_sid  "

    val saleNumRawRDD = hiveContext.sql(avgSaleNumSql).map { r =>
      (if (r.isNullAt(0) || r.get(0) == "null") -99L else r.getLong(0), (r.getString(1), r.getDouble(2)))
    }

    val tmp2 = categoryRawRDD.join(saleNumRawRDD).map { case (cate, ((lev1, lev1Name, lev2, lev2Name, lev3, lev3Name, lev4, lev4Name, lev5, lev5Name), (goodsId, saleNum))) =>
      Array((lev1, lev1Name, goodsId, saleNum / 30),
        (lev2, lev2Name, goodsId, saleNum / 30),
        (lev3, lev3Name, goodsId, saleNum / 30),
        (lev4, lev4Name, goodsId, saleNum / 30),
        (lev5, lev5Name, goodsId, saleNum / 30)).filter(_._1 != -1)
    }.flatMap(s => s)

    val insertAvgStockSql = s"update  $tableMysql  set avg_day_sale_sum = ? where category_sid = ? and  goods_sid = ? "

    tmp2.foreachPartition { partition =>
      val conn = MysqlConnection.connection
      conn.setAutoCommit(false)
      val pt = conn.prepareStatement(insertAvgStockSql)
      val n = 200
      var i = 0

      partition.foreach { case (lev, levName, goodsId, saleNum) =>
          pt.setDouble(1, (saleNum * 100 + 0.5).toInt.toDouble / 100)
          pt.setInt(2, lev.toInt)
          pt.setInt(3, goodsId.toInt)
          pt.addBatch()
          i += 1
          if (i % n == 0) {
            pt.executeBatch()
            conn.commit()
            Thread.sleep(100)
          }
      }
      pt.executeBatch()
      conn.commit()
    }

    val mysqlDF = hiveContext.jdbc(s"${MysqlConnection.url}?user=${MysqlConnection.user}&password=${MysqlConnection.password}", tableMysql)
    mysqlDF.registerTempTable("tmp")
    val tmpDF = hiveContext.sql("select category_sid, goods_sid, stock, avg_day_sale_sum from tmp where stock is not null and avg_day_sale_sum is not null  ")
    val ratioSql = s"update $tableMysql  set ratio = ? where category_sid = ? and goods_sid = ? "
    tmpDF.foreachPartition { partition =>
      val con = MysqlConnection.connection
      con.setAutoCommit(false)
      val pt = con.prepareStatement(ratioSql)
      var  i = 0
      val n = 2000
      partition.foreach{ r =>
        val cate = r.getInt(0)
        val goods = r.getInt(1)
        val stock = r.getInt(2)
        val saleQuantity = r.getDouble(3)

        pt.setDouble(1, if (saleQuantity <= 0.0) 0.00 else (stock * 100/ saleQuantity + 0.5).toInt.toDouble / 100)
        pt.setInt(2, cate)
        pt.setInt(3, goods)
        pt.addBatch()
        i += 1
        if (i % n == 0) {
          pt.executeBatch()
          con.commit()
          Thread.sleep(200)
        }
      }
      pt.executeBatch()
      con.commit()
    }


  }

  def bl_category_performance_category_operation(hiveContext: HiveContext): Unit = {

    val tableMysql = "bl_category_performance_category_operation"

    val categorySql = "  SELECT c.category_id, c.level1_id, c.level1_name, c.level2_id, c.level2_name, " +
                      "   c.level3_id, c.level3_name,  c.level4_id, c.level4_name, c.level5_id, c.level5_name " +
                      "   FROM idmdata.dim_management_category c "

    val insertSkuSql = s"replace into  $tableMysql   (category_sid, category_name, level, stock_sku_rate, cdate) values (?, ?, ?, ?, now()) "
    val categoryRawRDD = hiveContext.sql(categorySql).map { r =>
      ( if (r.isNullAt(0)) -1 else r.getLong(0),
        (if (r.isNullAt(1)) -1 else r.getLong(1), r.getString(2),
        if (r.isNullAt(3)) -1 else r.getLong(3), r.getString(4),
        if (r.isNullAt(5)) -1 else r.getLong(5), r.getString(6),
        if (r.isNullAt(7)) -1 else r.getLong(7), r.getString(8),
        if (r.isNullAt(9)) -1 else r.getLong(9), r.getString(10)) )
    }.distinct().cache()

    val goodsForSaleSql = "select category_id, sid, sale_status, stock from  recommendation.goods_avaialbe_for_sale_channel "

    val goodsRawRDD = hiveContext.sql(goodsForSaleSql).map { r =>
      (if (r.isNullAt(0) | r.get(0) == "null") -1 else r.getLong(0), r.getString(1), r.getInt(2), r.getInt(3))
    }.filter(_._1 != -1L).map { case (cate, goodsId, saleStatus, stock) => (cate, (goodsId, saleStatus, stock))}

    val tmp = categoryRawRDD.join(goodsRawRDD).map { case (cate, ((lev1, lev1Name, lev2, lev2Name, lev3, lev3Name, lev4, lev4Name, lev5, lev5Name), (goodsId, saleStatus, stock))) =>
      Array(((lev1, lev1Name, 1), (goodsId, if (saleStatus == 4) 1 else 0, if (stock == 1 && saleStatus == 4) 1 else 0)),
        ((lev2, lev2Name, 2), (goodsId, if (saleStatus == 4) 1 else 0, if (stock == 1 && saleStatus == 4) 1 else 0)),
        ((lev3, lev3Name, 3), (goodsId, if (saleStatus == 4) 1 else 0, if (stock == 1 && saleStatus == 4) 1 else 0)),
        ((lev4, lev4Name, 4), (goodsId, if (saleStatus == 4) 1 else 0, if (stock == 1 && saleStatus == 4) 1 else 0)),
        ((lev5, lev5Name, 5), (goodsId, if (saleStatus == 4) 1 else 0, if (stock == 1 && saleStatus == 4) 1 else 0))).filter(_._1._1 != 1L)
    }.flatMap(s => s).distinct().map(s => (s._1, (s._2._2, s._2._3))).reduceByKey((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2)).
      mapValues(s => if (s._1 == 0) 0.0 else s._2.toDouble / s._1)

    tmp.foreachPartition { partition =>
      val conn = MysqlConnection.connection
      conn.setAutoCommit(false)
      val pt = conn.prepareStatement(insertSkuSql)
      val n = 2000
      var i = 0
      partition.foreach { case ((cate, cateName, level), rate) =>
          pt.setInt(1, cate.toInt)
          pt.setString(2, cateName)
          pt.setInt(3, level)
          pt.setDouble(4, rate)
          pt.addBatch()
          i += 1
          if (i % n == 0) {
            pt.executeBatch()
            conn.commit()
            Thread.sleep(200)
          }
      }
      pt.executeBatch()
      conn.commit()

    }

    // 属性填充率
    val propertiesSql = "  SELECT category_id, avg(CAST(p.fill_product_num AS FLOAT) / CAST(p.product_num AS FLOAT))  " +
                        "  FROM idmdata.m_da_pcm_pro_props p  " +
                        "  join (SELECT max(dt) dt FROM idmdata.m_da_pcm_pro_props) q  on  p.dt=q.dt " +
                        "  GROUP BY p.category_id "
    val propertiesRawRDD = hiveContext.sql(propertiesSql).map { r =>
      (if (r.get(0) == "null" | r.isNullAt(0))  -99 else r.getString(0).toLong, if (r.isNullAt(1) | r.get(1) == "null") 0.0 else r.getDouble(1))
    }

    val tmp2 = categoryRawRDD.join(propertiesRawRDD).map { case (cate, ((lev1, lev1Name, lev2, lev2Name, lev3, lev3Name, lev4, lev4Name, lev5, lev5Name), value)) =>
          Array(((lev1, lev1Name, 1), Seq(value)),
            ((lev2, lev2Name, 2), Seq(value)),
            ((lev3, lev3Name, 3), Seq(value)),
            ((lev4, lev1Name, 4), Seq(value)),
            ((lev5, lev5Name, 5), Seq(value))).filter(_._1._1 != -1L)
        }.flatMap(s=>s).reduceByKey(_ ++ _)

    val insertAvgPropertyFillRate = s" update $tableMysql set ave_prop_fill_rate = ?  where category_sid = ? and level = ? "

    tmp2.foreachPartition { partition =>
      val conn = MysqlConnection.connection
      conn.setAutoCommit(false)
      val pt = conn.prepareStatement(insertAvgPropertyFillRate)
      val n = 200
      var i = 0

      partition.foreach { case ((cate, cateName, lev), avg_pro_fill) =>
          pt.setDouble(1, avg_pro_fill.sum / avg_pro_fill.length)
          pt.setInt(2, cate.toInt)
          pt.setInt(3, lev)
          pt.addBatch()
          i += 1
          if (i % n == 0) {
            pt.executeBatch()
            conn.commit()
            Thread.sleep(200)
          }
      }
      pt.executeBatch()
      conn.commit()

    }

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -90)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val dayBefore90 = sdf.format(cal.getTime)
    // 调价率
    val priceSql = " SELECT g2.category_id, count(t.goods_sid) FROM pdata.t02_pcm_price_h t  " +
                    "  JOIN  (SELECT DISTINCT g.sid, g.category_id FROM recommendation.goods_avaialbe_for_sale_channel g) g2  " +
                    "  ON g2.sid = t.goods_sid  " +
                    s"  WHERE  isnotnull(g2.category_id)  AND  t.start_dt >= '$dayBefore90'  " +
                    "  GROUP BY g2.category_id  "

    val priceRawRDD = hiveContext.sql(priceSql).map { r => (if (r.isNullAt(0)) -99 else r.getLong(0), if (r.isNullAt(1)) 0 else r.getLong(1)) }

    val priceTmp = categoryRawRDD.join(priceRawRDD).map { case (cate, ((lev1, lev1Name, lev2, lev2Name, lev3, lev3Name, lev4, lev4Name, lev5, lev5Name), count)) =>
      Array(((lev1, lev1Name, 1), count),
        ((lev2, lev2Name, 2), count),
        ((lev3, lev3Name, 3), count),
        ((lev4, lev1Name, 4), count),
        ((lev5, lev5Name, 5), count)).filter(_._1._1 != -1L)
    }.flatMap(s=>s).reduceByKey(_ + _)

    val insertPrice = s" update $tableMysql set ave_price_adjustment_time = ?  where category_sid = ? and level = ? "

    priceTmp.foreachPartition { partition =>
      val conn = MysqlConnection.connection
      conn.setAutoCommit(false)
      val pt = conn.prepareStatement(insertPrice)
      val n = 200
      var i = 0
      partition.foreach { case ((cate, cateName, lev), priceTime) =>
        pt.setDouble(1, priceTime)
        pt.setInt(2, cate.toInt)
        pt.setInt(3, lev)
        pt.addBatch()
        i += 1
        if (i % n == 0) {
          pt.executeBatch()
          conn.commit()
          Thread.sleep(200)
        }
      }
      pt.executeBatch()
      conn.commit()
    }

    // 上下架
    val onOffShelfSql = "   SELECT g2.category_id, count(s.goods_sid) FROM pdata.t02_pcm_chan_sale_h s   " +
                        "   JOIN  (SELECT DISTINCT g.sid, g.category_id FROM recommendation.goods_avaialbe_for_sale_channel g) g2 ON g2.sid = s.goods_sid   " +
                        s"  WHERE isnotnull(g2.category_id) AND s.sell_start_date >= '$dayBefore90'   " +
                        s"  GROUP BY g2.category_id   "

    val onOffShelfRawRDD = hiveContext.sql(onOffShelfSql).map { r =>
      (if (r.isNullAt(0)) -99 else r.getLong(0), if (r.isNullAt(1)) 0 else r.getLong(1))
    }

    val onOffShelfTmp = categoryRawRDD.join(onOffShelfRawRDD).map { case (cate, ((lev1, lev1Name, lev2, lev2Name, lev3, lev3Name, lev4, lev4Name, lev5, lev5Name), count)) =>
      Array(((lev1, lev1Name, 1), count),
        ((lev2, lev2Name, 2), count),
        ((lev3, lev3Name, 3), count),
        ((lev4, lev1Name, 4), count),
        ((lev5, lev5Name, 5), count)).filter(_._1._1 != -1L)
    }.flatMap(s=>s).reduceByKey(_ + _)

    val insertOnOffShelf = s" update  $tableMysql  set  ave_on_off_shelf_time = ?  where category_sid = ? and level = ?  "

    onOffShelfTmp.foreachPartition { partition =>
      val conn = MysqlConnection.connection
      conn.setAutoCommit(false)
      val pt = conn.prepareStatement(insertOnOffShelf)
      val n = 200
      var i = 0
      partition.foreach { case ((cate, cateName, lev), count) =>
        pt.setDouble(1, count)
        pt.setInt(2, cate.toInt)
        pt.setInt(3, lev)
        pt.addBatch()
        i += 1
        if (i % n == 0) {
          pt.executeBatch()
          conn.commit()
          Thread.sleep(200)
        }
      }
      pt.executeBatch()
      conn.commit()
    }

    //TODO  存售比指标
    val mysqlDF = hiveContext.load("jdbc", Map("url" -> s"${MysqlConnection.url}?user=${MysqlConnection.user}&password=${MysqlConnection.password}",
      "dbtable" -> "bl_category_performance_monthly_goods_shelf_sales_ratio"))
    mysqlDF.registerTempTable("tmp")
    val categoryRatio = hiveContext.sql("select category_sid, level, ratio  from  tmp  where  ratio  is  not  null  ")
    val varianceRDD = categoryRatio.map{ r =>
      ((r.getInt(0), r.getInt(1)), Seq(r.getDouble(2)))
    }.reduceByKey(_ ++ _).filter(s => !s._2.isEmpty && s._2.length >= 5).mapValues{ ratios =>
      val avg = ratios.sum / ratios.length
      val variance = ratios.foldLeft((0, 0.0))((a: (Int, Double), b: Double) => (a._1 + 1, a._2 + (b - avg) * (b - avg)))
      Math.pow(variance._2 / variance._1, 0.5)
    }.filter(_._2 != 0.0)

    varianceRDD.cache()
    val maxMinVariance = varianceRDD.map(s => (s._1._2, s._2)).aggregateByKey((Double.MaxValue, 0.0))((a: (Double, Double), b: Double) => (Math.min(a._1, b), Math.max(a._2, b)),
      (a: (Double, Double), b: (Double, Double)) => (Math.min(a._1, b._1), Math.max(a._1, b._2))).collectAsMap()

    val sqlTmp = "update bl_category_performance_category_operation set shelf_sale_ratio = ? where category_sid = ? and level = ? "
    varianceRDD.map{ variance =>
      val v = maxMinVariance(variance._1._2)
      (variance._1, if (v._1 - v._2 == 0.0) 1.0 else 1.0 - (variance._2 - v._1) / (v._2 - v._1)) }.
      foreachPartition { partition =>
          val conn = MysqlConnection.connection
          conn.setAutoCommit(false)
          val pt = conn.prepareStatement(sqlTmp)
          var i = 0
          val n = 2000
          partition.foreach{ case (cate, normalRatio) =>
            pt.setDouble(1, normalRatio)
            pt.setInt(2, cate._1)
            pt.setInt(3, cate._2)
            pt.addBatch()
            i += 1
            if (i % n == 0) {
              pt.executeBatch()
              conn.commit()
              Thread.sleep(150)
            }
        }
        pt.executeBatch()
        conn.commit()
      }






  }



  def bl_category_performance_category_monthly_hotcakes(hiveContext: HiveContext): Unit = {

    val tableName = "bl_category_performance_category_monthly_hotcakes"

    val saleSql = "SELECT  c.level1_id, c.level1_name, c.level2_id, c.level2_name, c.level3_id, c.level3_name, " +
      "         c.level4_id, c.level4_name, c.level5_id, c.level5_name, " +
      "        substring(o.sale_time, 0, 7), o.sale_price, o.goods_sid, o.goods_name, o.sale_sum, o.order_no " +
      "  FROM  recommendation.order_info o  " +
      "  inner JOIN  idmdata.dim_management_category c  ON c.category_id = o.category_id  and o.ORDER_STATUS NOT IN ('1001', '1029', '1100')  "

    val insertSql = s"replace into $tableName (category_sid, category_name, month, rank, goods_sid, goods_sale_name, goods_sale_price, sale_amount, cdate) " +
                      s"values (?,?,?,?,?,?,?,?,now()) "

    val shit = hiveContext.sql(saleSql).map { r =>
      (if (r.isNullAt(0)) -1 else r.getLong(0).toInt, r.getString(1),
        if (r.isNullAt(2)) -1 else r.getLong(2).toInt, r.getString(3),
        if (r.isNullAt(4)) -1 else r.getLong(4).toInt, r.getString(5),
        if (r.isNullAt(6)) -1 else r.getLong(6).toInt, r.getString(7),
        if (r.isNullAt(8)) -1 else r.getLong(8).toInt, r.getString(9),
        r.getString(10),
        if (r.isNullAt(11)) 0 else r.getDouble(11), r.getString(12), r.getString(13),
        if (r.isNullAt(14)) 0 else r.getDouble(14).toInt, r.getString(15))

    }.map { case (lev1, lev1Name, lev2, lev2Name, lev3, lev3Name, lev4, lev4Name, lev5, lev5Name, month, price, goodsId, goodsName, saleAmount, orderNo) =>
        Array( ((lev1, month, lev1Name, goodsId, goodsName, price), saleAmount, orderNo),
          ((lev3, month, lev3Name, goodsId, goodsName, price), saleAmount, orderNo),
          ((lev4, month, lev4Name, goodsId, goodsName, price), saleAmount, orderNo),
          ((lev5, month, lev5Name, goodsId, goodsName, price), saleAmount, orderNo) ).filter(_!= -1)
    }.flatMap(s => s).distinct.map(s => (s._1, s._2))

    val fuck = shit.reduceByKey(_ + _).map { case ((lev, month, levName, goodsId, goodsName, price), amount) => ((lev, month, levName), Seq((goodsId, goodsName, price, amount))) }.
      reduceByKey(_ ++ _).map { case (cate, goods) =>
        val sortedGoods = goods.sortWith(_._4 > _._4).take(20)
        for (i <- 0 until sortedGoods.length) yield {
          (cate._1, cate._2, cate._3, i + 1, sortedGoods(i))
        }
    }.flatMap(s => s).
      foreachPartition { partition =>
        val conn = MysqlConnection.connection
        conn.setAutoCommit(false)
        val pt = conn.prepareStatement(insertSql)
        val n = 200
        var i = 0
        partition.foreach { case (cate, month, cateName, rank, (goodsId, goodsName, price, amount)) =>
            pt.setInt(1, cate)
            pt.setString(2, cateName)
            pt.setString(3, month)
            pt.setInt(4, rank)
            pt.setString(5, goodsId)
            pt.setString(6, goodsName)
            pt.setDouble(7, price)
            pt.setInt(8, amount)
            pt.addBatch()
            i += 1
            if (i % n == 200) {
              pt.executeBatch()
              conn.commit()
            }
        }
        pt.executeBatch()
        conn.commit()
//        conn.setAutoCommit(true)
      }


  }

  def bl_category_performance_category_monthly_sales(hiveContext: HiveContext): Unit = {

    val tableName = "bl_category_performance_category_monthly_sales"

    val categorySql = "  SELECT c.category_id, c.level1_id, c.level1_name, c.level2_id, c.level2_name, " +
      "   c.level3_id, c.level3_name,  c.level4_id, c.level4_name, c.level5_id, c.level5_name " +
      "   FROM idmdata.dim_management_category c "

    val categoryRawRDD = hiveContext.sql(categorySql).map { r =>
      ( if (r.isNullAt(0)) -1 else r.getLong(0),
        (if (r.isNullAt(1)) -1 else r.getLong(1), r.getString(2),
          if (r.isNullAt(3)) -1 else r.getLong(3), r.getString(4),
          if (r.isNullAt(5)) -1 else r.getLong(5), r.getString(6),
          if (r.isNullAt(7)) -1 else r.getLong(7), r.getString(8),
          if (r.isNullAt(9)) -1 else r.getLong(9), r.getString(10)) )
    }.distinct().partitionBy(new HashPartitioner(10)).cache()


    val saleSql = "SELECT  o.category_id, substring(o.sale_time, 0, 7), o.sale_price, o.sale_sum, o.goods_sid  " +
                    " FROM  recommendation.order_info o where o.ORDER_STATUS NOT IN ('1001', '1029', '1100') "

    val insertSql = s"replace into $tableName  (category_sid, category_name, month, sales, sales_amount, sales_distinct_goods_amount, cdate) values(?, ?, ?, ?, ?, ?, now())  "

    val saleRDD = hiveContext.sql(saleSql).map { r =>
         (if (r.isNullAt(0) || r.get(2).toString.equalsIgnoreCase("null")) -99 else r.getLong(0),
           (r.getString(1),
           if (r.isNullAt(2) || r.get(2).toString.equalsIgnoreCase("null")) 0.0 else r.getDouble(2),
           if (r.isNullAt(3) || r.get(3).toString.equalsIgnoreCase("null")) 0.0 else r.getDouble(3),
           if (r.isNullAt(4) || r.get(4).toString.equalsIgnoreCase("null")) null else r.getString(4))) }.filter(s => s._2._4 != null)

    val result = categoryRawRDD.join(saleRDD).map { case (category, ((lev1, lev1Name, lev2, lev2Name, lev3, lev3Name, lev4, lev4Name, lev5, lev5Name), (month, price, amount, goodsId))) =>
          Array(((lev1, month, lev1Name), (price * amount, amount, Set(goodsId))),
                ((lev2, month, lev2Name), (price * amount, amount, Set(goodsId))),
                ((lev3, month, lev3Name), (price * amount, amount, Set(goodsId))),
                ((lev4, month, lev4Name), (price * amount, amount, Set(goodsId))),
                ((lev5, month, lev5Name), (price * amount, amount, Set(goodsId)))).filter(_._1._1 != -1)
      }.flatMap(s => s)

    val r = result.reduceByKey((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2, s1._3 ++ s2._3)).
      foreachPartition { partition =>
        val conn = MysqlConnection.connection
        conn.setAutoCommit(false)
        val pt = conn.prepareStatement(insertSql)
        val n = 200
        var i = 0
        partition.foreach { case ((levelId, month, levelName), (sales, amount, goodsSet)) =>
            pt.setInt(1, levelId.toInt)
            pt.setString(2, levelName)
            pt.setString(3, month)
            pt.setDouble(4, sales)
            pt.setInt(5, amount.toInt)
            pt.setInt(6, goodsSet.size)
            pt.addBatch()
            i += 1
            if (i % n == 0) {
              pt.executeBatch()
              conn.commit()
              Thread.sleep(300)
            }
        }
        pt.executeBatch()
        conn.commit()
//        conn.setAutoCommit(true)
      }

    // 动销率
    // months
    val cal = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyy-MM")
    var start = "2016-01"
    val end = sdf.format(new Date())
    cal.setTime(sdf.parse(start))
    val arrayBuffer = new ArrayBuffer[String]()
    while (start <= end) {
      arrayBuffer += start
      cal.add(Calendar.MONTH, 1)
      start = sdf.format(cal.getTime)
    }

    val sql =  " SELECT DISTINCT  c.level1_id, c.level1_name, c.level2_id, c.level2_name, c.level3_id, c.level3_name, c.level4_id, c.level4_name, c.level5_id, c.level5_name, " +
                " g.sid, o.goods_sid, o.order_no, substring(o.sale_time, 0, 7), o.sale_price * o.sale_sum  " +
                " FROM idmdata.dim_management_category c  " +
                " LEFT JOIN recommendation.goods_avaialbe_for_sale_channel g ON g.category_id = c.category_id AND c.product_id = g.pro_sid  " +
                " LEFT JOIN recommendation.order_info o ON o.goods_sid = g.sid where isnotnull(g.sid) "

    val insertDynamicSql =s"update  $tableName  set fifty_percent_cnr = ?, eighty_percent_cnr = ?, shelf_sales_ratio = ?, cdate = now()  " +
        s"where category_sid = ? and category_name = ? and month = ? "

    val rawRDD = hiveContext.sql(sql).map { r =>
      ( if (r.isNullAt(0)) -1 else r.getLong(0).toInt, r.getString(1),
        if (r.isNullAt(2)) -1 else r.getLong(2).toInt, r.getString(3),
        if (r.isNullAt(4)) -1 else r.getLong(4).toInt, r.getString(5),
        if (r.isNullAt(6)) -1 else r.getLong(6).toInt, r.getString(7),
        if (r.isNullAt(8)) -1 else r.getLong(8).toInt, r.getString(9),
        r.getString(10), r.getString(11), r.getString(12), r.getString(13),
        if (r.isNullAt(14)) 0.0 else r.getDouble(14))
    }.map { case (lev1, lev1Name, lev2, lev2Name, lev3, lev3Name, lev4, lev4Name, lev5, lev5Name, goodsId, orderGoodsId, orderNo, month, price) =>
        Array(((lev1, lev1Name), (Set(goodsId.toInt), Set((orderGoodsId, orderNo, price, month)))),
          ((lev2, lev2Name), (Set(goodsId.toInt), Set((orderGoodsId, orderNo, price, month)))),
          ((lev3, lev3Name), (Set(goodsId.toInt), Set((orderGoodsId, orderNo, price, month)))),
          ((lev4, lev4Name), (Set(goodsId.toInt), Set((orderGoodsId, orderNo, price, month)))),
          ((lev5, lev5Name), (Set(goodsId.toInt), Set((orderGoodsId, orderNo, price, month))))).filter(_._1._1 != -1)
    }.flatMap(s => s).reduceByKey((s1, s2) => (s1._1 ++ s2._1, s1._2 ++ s2._2)).map { case ((category, categoryName), (goods, orderGoods)) =>
        val validateGoods = goods.size
      val validataOrder = orderGoods.filter(_._2 != null)
        for (month <- arrayBuffer) yield {
          val tmp = validataOrder.filter(_._4 == month)
          if (tmp.isEmpty) {
            (category, categoryName, month, 0.0, 0.0, 0.0)
          } else {
            val sorted = tmp.groupBy(_._1).map(s => (s._1, s._2.foldLeft(0.0)((b: Double, item: (String, String, Double, String)) => b + item._3))).toArray.sortWith(_._2 > _._2)
            val saledGoodsCount = sorted.map(_._1).distinct.length.toDouble
            val sum = sorted.map(_._2).sum
            var n = 0.0
            val percent50 = for (t <- sorted if n < 0.5 *  sum) yield {n += t._2; t._1}
            n = 0.0
            val percent80 = for (t <- sorted if n < 0.8 *  sum) yield {n += t._2; t._1}
            (category, categoryName, month, saledGoodsCount / validateGoods, percent50.distinct.length.toDouble / validateGoods, percent80.distinct.length.toDouble / validateGoods)
          }
        }
    }.flatMap(s => s)

    rawRDD.foreachPartition { partition =>
      val conn = MysqlConnection.connection
      conn.setAutoCommit(false)
      val pt = conn.prepareStatement(insertDynamicSql)
      var i = 0
      val n = 200
      partition.foreach{ case (cate, cateName, month, dynamic, percent50, percent80) =>
          pt.setDouble(1, percent50)
          pt.setDouble(2, percent80)
          pt.setDouble(3, dynamic)
          pt.setInt(4, cate)
          pt.setString(5, cateName)
          pt.setString(6, month)
          pt.addBatch()
          i += 1
          if (i % n == 0) {
            pt.executeBatch()
            conn.commit()
            Thread.sleep(300)
          }
      }
      pt.executeBatch()
      conn.commit()
    }


  }

  def bl_category_performance_category_brand(hiveContext: HiveContext): Unit = {

    val tableName = "bl_category_performance_category_brand"
    val categoryBrandSql = "SELECT g.sid, g.brand_sid, g.cn_name, g.sale_status, g.stock, " +
      "c.level1_id, c.level1_name, c.level2_id, c.level2_name, c.level3_id, c.level3_name, " +
      "c.level4_id, c.level4_name, c.level5_id, c.level5_name, g.sale_price  " +
      "FROM  idmdata.dim_management_category c  " +
      "LEFT JOIN recommendation.goods_avaialbe_for_sale_channel g ON c.category_id = g.category_id "

    val sql = s"replace into $tableName (category_sid, category_name, brand_sid, brand_name, lowest_price, highest_price, " +
      s" total_goods_num, off_the_shelf_goods, out_of_stock_goods_num, cdate) values (?,?,?,?,?,?,?,?,?, now())  "

    val cateBrandRawRDD = hiveContext.sql(categoryBrandSql)

    val brandRDD = cateBrandRawRDD.map { r => (r.getString(0), r.getString(1), r.getString(2),
        if (r.isNullAt(3)) -1 else r.getInt(3),
        if (r.isNullAt(4)) -1 else r.getInt(4),
        if (r.isNullAt(5)) -1 else r.getLong(5).toInt, r.getString(6),
        if (r.isNullAt(7)) -1 else r.getLong(7).toInt, r.getString(8),
        if (r.isNullAt(9)) -1 else r.getLong(9).toInt, r.getString(10),
        if (r.isNullAt(11)) -1 else r.getLong(11).toInt, r.getString(12),
        if (r.isNullAt(13)) -1 else r.getLong(13).toInt, r.getString(14),
        if (r.isNullAt(15)) 0.0 else r.getDouble(15)) }.distinct()


    brandRDD.map { case (goodsId, brandId, brandName, saleStatus, stock, l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name, price) =>
        Array(((l5, brandId), (l5Name, brandName, Set(goodsId), if (saleStatus == 0) 1 else 0, if (stock == 1) 1 else 0, price, price)),
          ((l4, brandId), (l4Name, brandName, Set(goodsId), if (saleStatus == 0) 1 else 0, if (stock == 1) 1 else 0, price, price)),
          ((l3, brandId), (l3Name, brandName, Set(goodsId), if (saleStatus == 0) 1 else 0, if (stock == 1) 1 else 0, price, price)),
          ((l2, brandId), (l2Name, brandName, Set(goodsId), if (saleStatus == 0) 1 else 0, if (stock == 1) 1 else 0, price, price)),
          ((l1, brandId), (l1Name, brandName, Set(goodsId), if (saleStatus == 0) 1 else 0, if (stock == 1) 1 else 0, price, price))).filter(_._1!= -1)
    }.flatMap(s => s).reduceByKey((s1, s2) =>
      (s1._1, s1._2, s1._3 ++ s2._3, s1._4 + s2._4, s1._5 + s2._5, if (s1._6 < s2._6) s1._6 else s2._6, if (s1._6 > s2._6) s1._6 else s2._6)).
      foreachPartition { partition =>

        val conn = MysqlConnection.connection
        conn.setAutoCommit(false)
        val pt = conn.prepareStatement(sql)
        val n = 200
        var i = 0
        partition.foreach { case (cate, (cateName, brandName, totalGoodsSum, offShelfGoods, outOfStock, lowestPrice, highestPrice)) =>
            pt.setInt(1, cate._1)
            pt.setString(2, cateName)
            try {
              if (cate._2 == null) pt.setInt(3, -1) else if (cate._2.length == 0 | cate._2.equalsIgnoreCase("null")) pt.setInt(3, -1) else pt.setInt(3, cate._2.toInt)
            }catch {
              case e: Throwable => println(e)
            }
              pt.setString(4, brandName)
              pt.setDouble(5, lowestPrice)
              pt.setDouble(6, highestPrice)
              pt.setInt(7, totalGoodsSum.size)
              pt.setInt(8, offShelfGoods)
              pt.setInt(9, outOfStock)
              pt.addBatch()
              i += 1
              if (i % n == 0) {
                pt.executeBatch()
                conn.commit()
                Thread.sleep(500)
              }
            }
        pt.executeBatch()
        conn.commit()



    }

  }


  def bl_category_performance_basic(hiveContext: HiveContext): Unit = {


    val tableName1 = "recommend_system.bl_category_performance_basic"

    val categorySql = "  SELECT c.category_id, c.level1_id, c.level1_name, c.level2_id, c.level2_name, " +
      "   c.level3_id, c.level3_name,  c.level4_id, c.level4_name, c.level5_id, c.level5_name " +
      "   FROM idmdata.dim_management_category c "
    val goodsForSaleSql = " SELECT g.category_id, g.sid, g.brand_sid, g.sale_price, g.sale_status, g.stock from recommendation.goods_avaialbe_for_sale_channel g "

    val categoryRawRDD = hiveContext.sql(categorySql).map { r =>
      ( if (r.isNullAt(0)) -1 else r.getLong(0),
        (if (r.isNullAt(1)) -1 else r.getLong(1), r.getString(2),
          if (r.isNullAt(3)) -1 else r.getLong(3), r.getString(4),
          if (r.isNullAt(5)) -1 else r.getLong(5), r.getString(6),
          if (r.isNullAt(7)) -1 else r.getLong(7), r.getString(8),
          if (r.isNullAt(9)) -1 else r.getLong(9), r.getString(10)) )
    }.distinct().partitionBy(new HashPartitioner(10)).cache()

    val goodsRawRDD0 = hiveContext.sql(goodsForSaleSql).map { r =>
      (if (r.isNullAt(0) || r.get(0).toString.equalsIgnoreCase("null")) -99 else r.getLong(0), (r.getString(1), r.getString(2),
        if (r.isNullAt(3) || r.get(3).toString.equalsIgnoreCase("null")) 0.0 else r.getDouble(3),
        if (r.isNullAt(4) || r.get(4).toString.equalsIgnoreCase("null")) -1 else r.getInt(4),
        if (r.isNullAt(5) || r.get(5).toString.equalsIgnoreCase("null")) -1 else r.getInt(5)))
    }.filter(s => s._2._1 != null && !s._2._1.equalsIgnoreCase("null") && s._2._2 != null && !s._2._2.equalsIgnoreCase("null")).distinct()

    val tmp0 = categoryRawRDD.join(goodsRawRDD0).map { case (cate, ((l1, l1Name, l2, l2Name, l3, l3Name, l4, l4Name, l5, l5Name), (goodsId, brandId, salePrice, saleStatus, stock))) =>
      Array(((l5, l5Name, 5, l4, l1Name + ">" + l2Name + ">" + l3Name + ">" + l4Name),  (1, if (saleStatus == 0) 1 else 0, if (stock == 0 && saleStatus == 4) 1 else 0, Set(brandId), salePrice, salePrice)),
            ((l4, l4Name, 4, l3, l1Name + ">" + l2Name + ">" + l3Name),  (1, if (saleStatus == 0) 1 else 0, if (stock == 0 && saleStatus == 4) 1 else 0, Set(brandId), salePrice, salePrice)),
            ((l3, l3Name, 3, l2, l1Name + ">" + l2Name), (1, if (saleStatus == 0) 1 else 0, if (stock == 0 && saleStatus == 4) 1 else 0, Set(brandId), salePrice, salePrice)),
            ((l2, l2Name, 2, l1, l1Name), (1, if (saleStatus == 0) 1 else 0, if (stock == 0 && saleStatus == 4) 1 else 0, Set(brandId), salePrice, salePrice)),
            ((l1, l1Name, 1, -99L, l1Name), (1, if (saleStatus == 0) 1 else 0, if (stock == 0 && saleStatus == 4) 1 else 0, Set(brandId), salePrice, salePrice))).filter(_._1._1 != -1)
    }.flatMap(s => s).reduceByKey((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2, s1._3 + s2._3, s1._4 ++ s2._4, Math.min(s1._5, s2._5), Math.max(s1._6, s2._6)))


      tmp0.foreachPartition { partition =>

      val sql = s"replace into $tableName1 (category_sid, category_name, level, parent_sid, category_tree, total_goods_num, off_the_shelf_goods_num, out_of_stock_goods_num, " +
        s" brand_num, lowest_price, highest_price, cdate)  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())"
      val conn = MysqlConnection.connection
      conn.setAutoCommit(false)
      val pt = conn.prepareStatement(sql)
      val n = 200
      var i = 0
      partition.foreach { case ((cate, cateName, level, parentId, categoryTree), (goodsNum, offShelfGoods, outOfStock, brandNum, lowestPrice, highestPrice)) =>
        pt.setInt(1, cate.toInt)
        pt.setString(2, cateName)
        pt.setInt(3, level)
        if (parentId != null) pt.setInt(4, parentId.toInt)
        if (categoryTree != null) pt.setString(5, categoryTree)
        pt.setInt(6, goodsNum)
        pt.setInt(7, offShelfGoods)
        pt.setInt(8, outOfStock)
        pt.setInt(9, brandNum.size)
        pt.setDouble(10, lowestPrice)
        pt.setDouble(11, highestPrice)
        i += 1
        pt.addBatch()
        if (i % n == 0) {
          pt.executeBatch()
          conn.commit()
          Thread.sleep(1000)
        }
      }
      pt.executeBatch()
      conn.commit()

    }



  }



}
