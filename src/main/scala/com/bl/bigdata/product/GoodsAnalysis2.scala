package com.bl.bigdata.product

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.bl.bigdata.util.SparkFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, io}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.logging.log4j.LogManager

import scala.collection.mutable

/**
  * Created by MK33 on 2016/8/10.
  */
object GoodsAnalysis2 {

  val logger = LogManager.getLogger(this.getClass.getName)

  def main(args: Array[String]) {

    val hiveContext = SparkFactory.getHiveContext
    // ==============  read / write hBase configuration ==========
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(TableOutputFormat.OUTPUT_TABLE, "category_analysis2")
    hBaseConf.set(TableInputFormat.INPUT_TABLE, "category_analysis2")
    hBaseConf.set("hbase.zookeeper.quorum", "slave23.bl.bigdata,slave24.bl.bigdata,slave21.bl.bigdata,slave22.bl.bigdata,slave25.bl.bigdata")
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    val job = Job.getInstance(hBaseConf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[Put])
    job.setOutputFormatClass(classOf[TableOutputFormat[Put]])

    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val oneMonthAgo = {cal.add(Calendar.MONTH, -1); sdf.format(cal.getTime)}
    val threeMonthAgo = {cal.add(Calendar.MONTH, -2); sdf.format(cal.getTime)}

    // ======================   product information   =======================
    val productBytes = Bytes.toBytes("product")

    val sql0 = "select category_id, count(distinct sid), count(distinct brand_sid), count(distinct pro_sid) " +
      "from recommendation.goods_avaialbe_for_sale_channel  group by category_id "
    val goodsRawRDD = hiveContext.sql(sql0).map { row =>
      if (row.anyNull) null else (row.getLong(0).toString, (row.getLong(1).toInt, row.getLong(2).toInt, row.getLong(3).toInt))
    }
    logger.info("invalidate goods records: " + goodsRawRDD.filter(_ != null).count)
    goodsRawRDD.filter(_ != null).map { case (cate, (goodsCount, brandCount, productCount)) =>
        val put = new Put(Bytes.toBytes(cate))
        put.addColumn(productBytes, Bytes.toBytes("goods_count"), Bytes.toBytes(goodsCount))
        put.addColumn(productBytes, Bytes.toBytes("brand_count"), Bytes.toBytes(brandCount))
        put.addColumn(productBytes, Bytes.toBytes("product_count"), Bytes.toBytes(productCount))
      (new io.ImmutableBytesWritable(Bytes.toBytes(cate)), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    val categoryLevSql = "select category_id, category_name, level1_id, level2_id, level3_id from recommendation.dim_category  "
    val categoryLevRawRDD = hiveContext.sql(categoryLevSql).map { row =>
      if (row.isNullAt(0)) null
      else if (!row.isNullAt(4) && row.getLong(0) != row.getLong(4)) (row.getLong(0).toString, row.getLong(4).toString)
      else if (!row.isNullAt(3) && row.getLong(0) != row.getLong(3)) (row.getLong(0).toString, row.getLong(3).toString)
      else if (!row.isNullAt(2) && row.getLong(0) != row.getLong(2)) (row.getLong(0).toString, row.getLong(2).toString)
      else if (!row.isNullAt(1) && row.getLong(0) != row.getLong(1)) (row.getLong(0).toString, row.getLong(1).toString)
      else null
    }
    val categoryLevRDD = categoryLevRawRDD.filter( _ != null).map(s => (s._1, s._2))
    logger.info("Invalid category level number count:  " + categoryLevRawRDD.filter(_ == null).count())
    logger.info("validate category level number count:  " + categoryLevRDD.count())

    val parentGoodsInfo = goodsRawRDD.filter(_!=null).join(categoryLevRDD).map { case (cate, ((goodsCount, brandCount, productCount), parentCate)) =>
      (parentCate, Seq((cate, goodsCount, brandCount, productCount)))
    }.reduceByKey(_ ++ _).map { case (parentCate, goods) =>
        val goodsCount = goods.map(_._2).sum
        val brandsCount = goods.map(_._3).sum
        val productCount = goods.map(_._4).sum
        val r = for ( s <- goods) yield {
          val put = new Put(Bytes.toBytes(s._1))
          put.addColumn(productBytes, Bytes.toBytes("goods_count_proportion"), Bytes.toBytes(s._2.toDouble / goodsCount))
          put.addColumn(productBytes, Bytes.toBytes("brand_count_proportion"), Bytes.toBytes(s._3.toDouble / brandsCount))
          put.addColumn(productBytes, Bytes.toBytes("product_count_proportion"), Bytes.toBytes(s._4.toDouble / productCount))
          (new ImmutableBytesWritable(Bytes.toBytes(s._1)), put)
        }
        r
    }.flatMap(s => s).saveAsNewAPIHadoopDataset(job.getConfiguration)

    //   ============  price zone  ===========
    val sql1 = "select category_id, sale_price  from recommendation.goods_avaialbe_for_sale_channel  "
    val goodsRawRDD2 = hiveContext.sql(sql1).map { row =>
      if (row.anyNull) null else (row.getLong(0).toString , row.getDouble(1))
    }
    val validateGoodsRDD = goodsRawRDD2.filter(_!=null)
    val priceZoneRDD = validateGoodsRDD.map(s => (s._1, Seq(s._2))).reduceByKey(_ ++ _).mapValues(s => (s.min, s.max))

    priceZoneRDD.map{ case (cate, (min, max)) =>
        val put = new Put(Bytes.toBytes(cate))
        put.addColumn(productBytes, Bytes.toBytes("min_price"), Bytes.toBytes(min))
        put.addColumn(productBytes, Bytes.toBytes("max_price"), Bytes.toBytes(max))
        val delt = (max - min) / 10
        val r = for (i <- 0 until 11) yield {
          min + delt * i
        }
      put.addColumn(productBytes, Bytes.toBytes("price_zone"), Bytes.toBytes(r.mkString(",")))
      (new ImmutableBytesWritable(Bytes.toBytes(cate)), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    // 品类最高价格最低价格保存在 hbase 中
    val priceZoneRDDTmp = hiveContext.sparkContext.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2).map { result =>
      val key = Bytes.toString(result.getRow)
      val priceZoneStr = result.getValue(productBytes, Bytes.toBytes("price_zone"))

      if (priceZoneStr == null) null else (key, Bytes.toString(priceZoneStr).split(",").map(_.toDouble))
    }.filter(_!=null)
    priceZoneRDDTmp.cache()

    val sql2 = " select distinct category_id, sid, brand_sid, sale_price from recommendation.goods_avaialbe_for_sale_channel "

    val goodsRawRDD3 = hiveContext.sql(sql2).map(row => if (row.anyNull) null else (row.getLong(0).toString, (row.getString(1), row.getString(2), row.getDouble(3))))
    goodsRawRDD3.filter(_!=null).map(s => (s._1, Seq(s._2))).reduceByKey(_ ++ _).join(priceZoneRDDTmp).map { case (cate, (goods, priceArray)) =>
        val put = new Put(Bytes.toBytes(cate))
        val r = for (i <- 1 until priceArray.length) yield {
          val n = goods.filter(s => s._3 >= priceArray(i - 1) && s._3 < priceArray(i))
          if (n!=null) (i + ":" + n.map(_._1).distinct.length, i + ":" + n.map(_._2).distinct.length)
          else (i + ":" + 0, i + ":" + 0)
        }
        put.addColumn(productBytes, Bytes.toBytes("price_zone_goods_count"), Bytes.toBytes(r.map(_._1).mkString("#")))
        put.addColumn(productBytes, Bytes.toBytes("price_zone_brand_count"), Bytes.toBytes(r.map(_._2).mkString("#")))
      (new ImmutableBytesWritable(Bytes.toBytes(cate)), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)





    val oneMonthAgoOrderSql = " select  category_id,  sum(sale_price), sum(sale_sum), count(distinct member_id)  " +
      s" from recommendation.order_info where  dt >= ${oneMonthAgo}  and ORDER_STATUS NOT IN ('1001', '1029', '1100') group by category_id "

    val threeMothAgoOrderSql = " select  category_id,  sum(sale_price), sum(sale_sum), count(distinct member_id) " +
        s" from recommendation.order_info where  dt >= ${threeMonthAgo}  and ORDER_STATUS NOT IN ('1001', '1029', '1100')  group by category_id"

    val orderSql = " select  category_id,  sale_price, sale_sum, distinct member_id  " +
      s" from recommendation.order_info where  ORDER_STATUS NOT IN ('1001', '1029', '1100')  "

    val oneMonthRDD = hiveContext.sql(oneMonthAgoOrderSql).map { row =>
      if (row.anyNull) null else (row.getLong(0).toString, (row.getDouble(1), row.getDouble(2), row.getLong(3).toInt))
    }

    oneMonthRDD.filter(_!=null).map { s =>
      val put = new Put(Bytes.toBytes(s._1))
      put.addColumn(productBytes, Bytes.toBytes("one_month_sale_money"), Bytes.toBytes(s._2._1))
      put.addColumn(productBytes, Bytes.toBytes("one_month_sale_number"), Bytes.toBytes(s._2._2))
      put.addColumn(productBytes, Bytes.toBytes("one_month_custom"), Bytes.toBytes(s._2._3))
      (new ImmutableBytesWritable(Bytes.toBytes(s._1)), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)


    val threeMonthRDD = hiveContext.sql(threeMothAgoOrderSql).map{ row =>
      if (row.anyNull) null else (row.getLong(0).toString, (row.getDouble(1), row.getDouble(2), row.getLong(3).toInt))
    }

    logger.info(s"invalid three month order number is :  ${threeMonthRDD.filter(_==null).count()}")

    threeMonthRDD.filter(_!=null).map { s =>
      val put = new Put(Bytes.toBytes(s._1))
      put.addColumn(productBytes, Bytes.toBytes("three_month_sale_money"), Bytes.toBytes(s._2._1))
      put.addColumn(productBytes, Bytes.toBytes("three_month_sale_number"), Bytes.toBytes(s._2._2))
      put.addColumn(productBytes, Bytes.toBytes("three_month_custom"), Bytes.toBytes(s._2._3))
      (new ImmutableBytesWritable(Bytes.toBytes(s._1)), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)




    val buyAgain = " select category_id , count(member_id) " +
      s"  from recommendation.order_info where  dt >= ${threeMonthAgo}  " +
      s"  and  ORDER_STATUS NOT IN ('1001', '1029', '1100')  GROUP BY category_id having count(member_id) >=  2 "

    val buyAgainRDD = hiveContext.sql(buyAgain).map(row => if (row.anyNull) null else (row.getLong(0).toString, row.getLong(1).toInt))
    logger.info("invalid buy again record number is : " + buyAgainRDD.filter(_== null).count())
    hiveContext.sparkContext.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2).map { result =>
      val key = Bytes.toString(result.getRow)
      val customNum = result.getValue(productBytes, Bytes.toBytes("three_month_custom"))
      if (customNum == null) null
      else (key, Bytes.toInt(customNum))
    }.filter(_!=null).join(buyAgainRDD.filter(_!=null)).map { case (cate, (n1, n2)) =>
        val put = new Put(Bytes.toBytes(cate))
        put.addColumn(productBytes, Bytes.toBytes("buy_once_more_rate"), Bytes.toBytes(n2.toDouble / n1))
      (new ImmutableBytesWritable(Bytes.toBytes(cate)), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)


    //  ====================   动销率  =================
    val goodsSql2 = "  select category_id, count(distinct goods_sid), count(distinct brand_sid)  " +
            s"  from recommendation.order_info where  dt >= ${oneMonthAgo}  and  ORDER_STATUS NOT IN ('1001', '1029', '1100')  " +
            s"  GROUP BY category_id  "
    val goodsSql3 = "  select category_id, count(distinct goods_sid), count(distinct brand_sid)  " +
            s"  from recommendation.order_info where  dt >= ${threeMonthAgo}  and  ORDER_STATUS NOT IN ('1001', '1029', '1100')  " +
            s"  GROUP BY category_id  "

    val soldGoodsCountRDD = hiveContext.sql(goodsSql2).map(row => if (row.anyNull) null else (row.getLong(0).toString, (row.getLong(1).toInt, row.getLong(2).toInt))).filter(_!=null)
    val hbaseGoodsRDD = hiveContext.sparkContext.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2).map { result =>
      val cate = Bytes.toString(result.getRow)
      val n1 = result.getValue(productBytes, Bytes.toBytes("goods_sum"))
      val n2 = result.getValue(productBytes, Bytes.toBytes("brand_sum"))
      if (n1 == null | n2 == null) null
      else (cate, (Bytes.toInt(n1), Bytes.toInt(n2)))
    }.filter(_!= null)
      hbaseGoodsRDD.join(soldGoodsCountRDD).map{ case (cate, ((soldGoods, soldBrand), (goods, brand))) =>
        val put = new Put(Bytes.toBytes(cate))
        put.addColumn(productBytes, Bytes.toBytes("goods_dynamic_pin_ratio_30"), Bytes.toBytes(soldGoods.toDouble / goods))
        put.addColumn(productBytes, Bytes.toBytes("brand_dynamic_pin_ratio_30"), Bytes.toBytes(soldBrand.toDouble / goods))
      (new ImmutableBytesWritable(Bytes.toBytes(cate)), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    val soldGoodsCountRDD3 = hiveContext.sql(goodsSql3).map(row => if (row.anyNull) null else (row.getLong(0).toString, (row.getLong(1).toInt, row.getLong(2).toInt))).filter(_!=null)
    hbaseGoodsRDD.join(soldGoodsCountRDD3).map{ case (cate, ((soldGoods, soldBrand), (goods, brand))) =>
      val put = new Put(Bytes.toBytes(cate))
      put.addColumn(productBytes, Bytes.toBytes("goods_dynamic_pin_ratio_90"), Bytes.toBytes(soldGoods.toDouble / goods))
      put.addColumn(productBytes, Bytes.toBytes("brand_dynamic_pin_ratio_90"), Bytes.toBytes(soldBrand.toDouble / goods))
      (new ImmutableBytesWritable(Bytes.toBytes(cate)), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    // ========   销售额 销售数量平均增幅   ======
    hiveContext.sparkContext.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2).map { result =>
      val key = result.getRow
      val saleMoney30 = result.getValue(productBytes, Bytes.toBytes("one_month_sale_money"))
      val saleMoney90 = result.getValue(productBytes, Bytes.toBytes("three_month_sale_money"))
      val saleAmt30 = result.getValue(productBytes, Bytes.toBytes("one_month_sale_number"))
      val saleAmt90 = result.getValue(productBytes, Bytes.toBytes("three_month_sale_number"))
      val put = new Put(key)
      if (saleAmt30 != null && saleAmt90 != null) {
        put.addColumn(productBytes, Bytes.toBytes("growth_of_sales"), Bytes.toBytes(3 * Bytes.toInt(saleAmt30).toDouble / Bytes.toInt(saleAmt90)))
      }
      if (saleMoney30 != null && saleMoney90 != null) {
        put.addColumn(productBytes,  Bytes.toBytes("growth_of_sales_number"), Bytes.toBytes(3 * Bytes.toInt(saleMoney30).toDouble / Bytes.toInt(saleMoney90)))
      }
      if (put.isEmpty) null else
      (new io.ImmutableBytesWritable(key), put)
    }.filter(_!=null).saveAsNewAPIHadoopDataset(job.getConfiguration)

    // =========================
    val goodsSql4 = " select  category_id, sale_time, member_id, goods_sid, sale_price, order_no  " +
                    s" from recommendation.order_info where  dt >= ${threeMonthAgo}  and  ORDER_STATUS NOT IN ('1001', '1029', '1100') "

    val goodsRawRDD5 = hiveContext.sql(goodsSql4).map { row => if (row.anyNull) null
    else  (row.getLong(0).toString, Seq((row.getString(1), row.getString(2), row.getString(3), row.getDouble(4), row.getString(5)))) }.filter(_!=null).reduceByKey(_ ++_).map { case (cate, goods) =>
        if (goods.isEmpty) null else {
          val put = new Put(Bytes.toBytes(cate))
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          //  计算品类平均购买周期, 单位是天
          val averageTime = goods.map(s => (s._1, s._2)).groupBy(_._2).filter(_._2.size >= 2)
          if (!averageTime.isEmpty) {
            val tmp0 = averageTime.map { case (member, dayTimeString) =>
              val t = dayTimeString.map(s=>sdf.parse(s._1).getTime).sorted
              (t.last - t.head, t.length - 1)
            }.foldLeft((0L, 0))((a: (Long, Int), b: (Long, Int)) => (a._1 + b._1, a._2 + b._2))
            val averageBuyPeriod = tmp0._1.toDouble / tmp0._2 / 24 / 3600000
            put.addColumn(productBytes, Bytes.toBytes("average_period"), Bytes.toBytes(averageBuyPeriod))
          }

          val minPrice = goods.minBy(_._4)._4
          val maxPrice = goods.maxBy(_._4)._4
          val goodsCountMap = mutable.Map[String, Int]()
          goods.map(_._3).map ( g => goodsCountMap(g) = goodsCountMap.getOrElse(g, 0) + 1 )
          val hotsales = if (goodsCountMap.isEmpty) "null"  else goodsCountMap.toVector.sortWith( _._2 > _._2).take(10).map(_._1).mkString(",")

          // 平均客单价 订单商品数
          val memberOrders = goods.map(s => (s._5, (s._3, s._4))).groupBy(_._1)
          val averageOrderGoodsNum = memberOrders.map(s=> s._2.length).sum.toDouble / memberOrders.size
          val averageOrderPrice = memberOrders.map(_._2.map(_._2._2).sum).sum / memberOrders.size

          put.addColumn(productBytes, Bytes.toBytes("average_order_price"), Bytes.toBytes(averageOrderPrice))
          put.addColumn(productBytes, Bytes.toBytes("average_order_number"), Bytes.toBytes(averageOrderGoodsNum))
          put.addColumn(productBytes, Bytes.toBytes("lowest_goods_price"), Bytes.toBytes(minPrice))
          put.addColumn(productBytes, Bytes.toBytes("higest_goods_price"), Bytes.toBytes(maxPrice))
          put.addColumn(productBytes, Bytes.toBytes("hotsale_top_10"), Bytes.toBytes(hotsales))
          (new ImmutableBytesWritable(Bytes.toBytes(cate)), put)
        }

    }.filter(_ != null).saveAsNewAPIHadoopDataset(job.getConfiguration)


    // ==============  price zone compute  =================
    val orderSql4 = "  select category_id, goods_sid, brand_sid , sale_price  " +
      s"  from recommendation.order_info where  dt >= ${threeMonthAgo}  and  ORDER_STATUS NOT IN ('1001', '1029', '1100')  "
    val threeOrderRDD = hiveContext.sql(orderSql4).map(row=> if (row.anyNull) null else (row.getLong(0).toString, (row.getString(1), row.getString(2), row.getDouble(3)))).filter(_ != null)

    val tmpPrizeZone = hiveContext.sparkContext.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2).map { r =>
      val key = r.getRow
      val g = r.getValue(productBytes, Bytes.toBytes("goods_count"))
      val b = r.getValue(productBytes, Bytes.toBytes("brand_count"))
//      val p = r.getValue(productBytes, Bytes.toBytes("product_count"))
      val priceZone = r.getValue(productBytes, Bytes.toBytes("price_zone"))

      if (g == null | b == null  | priceZone == null) null
      else (Bytes.toString(key), (Bytes.toInt(g), Bytes.toInt(b), Bytes.toString(priceZone).split(",").map(_.toDouble)))
    }.filter(_ != null)

    threeOrderRDD.map(s=> (s._1, Seq(s._2))).reduceByKey(_ ++ _).join(tmpPrizeZone).map { case (cate, (goods, (g, b, priceZone))) =>
        val maxPrice = goods.maxBy(_._3)._3
        val minPrice = goods.minBy(_._3)._3

        val c = for (i <- 1 until priceZone.length) yield {
          val tmp = goods.filter(s=> s._3 >= priceZone(i - 1) && s._3 < priceZone(i))
          val g0 = tmp.map(_._1).distinct.length
          val b0 = tmp.map(_._2).distinct.length
          val p0 = tmp.map(_._3).sum
          (i + ":" + g0, i + ":"+ b0, i+";"+ p0, i + ":" + (g0.toDouble / g), i + ":" + (b0.toDouble / b))
        }

        val put = new Put(Bytes.toBytes(cate))
        put.addColumn(productBytes, Bytes.toBytes("price_zone_sale_goods_count_90"), Bytes.toBytes(c.map(_._1).mkString("#")))
        put.addColumn(productBytes, Bytes.toBytes("price_zone_sale_brand_count_90"), Bytes.toBytes(c.map(_._2).mkString("#")))
        put.addColumn(productBytes, Bytes.toBytes("price_zone_sales_90"), Bytes.toBytes(c.map(_._3).mkString("#")))
        put.addColumn(productBytes, Bytes.toBytes("price_zone_goods_dynamic_90"), Bytes.toBytes(c.map(_._4).mkString("#")))
        put.addColumn(productBytes, Bytes.toBytes("price_zone_brand_dynamic_90"), Bytes.toBytes(c.map(_._5).mkString("#")))

        val sales = goods.map(_._3).sum
        val goodsCount = goods.map(_._1).distinct.length
        val sortedBySales = goods.map(s => (s._1, s._3)).groupBy(_._1).map ( s => (s._1, s._2.map(_._2).sum)).toArray.sortWith(_._2 > _._2)
        val tenPercent = sortedBySales.take(if (goodsCount / 10 == 0) 1 else goodsCount / 10).map(_._2).sum / sales
        val twentyPercent = sortedBySales.take((if (goodsCount / 10 == 0) 1 else 2 * goodsCount / 10)).map(_._2).sum / sales
        put.addColumn(productBytes, Bytes.toBytes("ten_percent_goods_sales"), Bytes.toBytes(tenPercent))
        put.addColumn(productBytes, Bytes.toBytes("twenty_percent_goods_sales"), Bytes.toBytes(twentyPercent))

      (new ImmutableBytesWritable(Bytes.toBytes(cate)), put)


    }.saveAsNewAPIHadoopDataset(job.getConfiguration)


    // sales money
    val orderSql5 = "  select  goods_sid , sale_price, dt  " +
      s"  from recommendation.order_info where  ORDER_STATUS NOT IN ('1001', '1029', '1100')  "

    val tt = hiveContext.sql(orderSql5).map(row => if (row.anyNull) null else (row.getString(0), row.getDouble(1), row.getString(2))).filter(_!=null)

    val sum = tt.map(_._2).sum()
    val goodsNumber = tt.map(_._1).distinct().count()
    val goodsSales = tt.filter(_._3 >= threeMonthAgo).map(s=>(s._1, s._2)).reduceByKey(_ + _).top(goodsNumber.toInt / 10).map(_._2).sum
    val goodsSales2 = tt.filter(_._3 >= threeMonthAgo).map(s=>(s._1, s._2)).reduceByKey(_ + _).top(2 * goodsNumber.toInt / 10).map(_._2).sum
























  }

  // 统计每个价格带的特征
//  def calculatorPriceZoneData(priceZoneRdd: RDD[(String, (Double, Double))], targetRdd: RDD[(String, (Double, String))]): RDD[(String, String)] = {
//    targetRdd.map(s => (s._1, Seq(s._2))).reduceByKey(_ ++ _).join(priceZoneRdd).map { case (category, (goods, (min, max))) =>
//        val delt = (max - min) / 10
//        val r = for (i <- 0 until 10) yield {
//          val n = goods.count(s => (s._1 >= (min + i * delt - delt)) && (s._1 < (min + i * delt)))
//          i + ":" + n
//        }
//      (category, r.mkString(","))
//    }
//  }
//
//  // 统计每个价格带的特征
//  def calculatorPriceZoneData(priceZoneRdd: RDD[(String, (Double, Double))], targetRdd: RDD[(String, (Double, Double))]): RDD[(String, String)] = {
//    targetRdd.map(s => (s._1, Seq(s._2))).reduceByKey(_ ++ _).join(priceZoneRdd).map { case (category, (goods, (min, max))) =>
//      val delt = (max - min) / 10
//      val r = for (i <- 0 until 10) yield {
//        val n = goods.filter(s => (s._1 >= (min + i * delt - delt)) && (s._1 < (min + i * delt))).map(_._2).sum
//        i + ":" + n
//      }
//      (category, r.mkString(","))
//    }
//  }
//
//  // 统计每个价格带的特征
//  def calculatorPriceZoneData(priceZoneRdd: RDD[(String, (Double, Double))], targetRdd: RDD[(String, (Double, Int))]): RDD[(String, String)] = {
//    targetRdd.map(s => (s._1, Seq(s._2))).reduceByKey(_ ++ _).join(priceZoneRdd).map { case (category, (goods, (min, max))) =>
//      val delt = (max - min) / 10
//      val r = for (i <- 0 until 10) yield {
//        val n = goods.filter(s => (s._1 >= (min + i * delt - delt)) && (s._1 < (min + i * delt))).map(_._2).sum
//        i + ":" + n
//      }
//      (category, r.mkString(","))
//    }
//  }






}
