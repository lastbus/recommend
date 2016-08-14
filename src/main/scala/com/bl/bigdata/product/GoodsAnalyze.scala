package com.bl.bigdata.product

import java.util.{Calendar, Date}

import com.bl.bigdata.util.{DataBaseUtil, SparkFactory}
import org.apache.hadoop.hbase.{HBaseConfiguration, io}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.logging.log4j.LogManager

/**
  * Created by MK33 on 2016/8/3.
  */
object GoodsAnalyze {
  val logger = LogManager.getLogger(this.getClass.getName)

  def main(args: Array[String]) {

    val categorySql2 = " select category_id, pro_sid, brand_sid, sale_status, sale_price " +
      "  from recommendation.goods_avaialbe_for_sale_channel " +
      "  where sale_status = 4 and stock = 1 "
    val categoryLevSql = "select category_id, category_name, level1_id, level2_id, level3_id from recommendation.dim_category  "
    SparkFactory.getSparkContext("goods analyze")
    val hiveContext = SparkFactory.getHiveContext
    // 加载商品类别数据
    val categoryLevRawRDD = hiveContext.sql(categoryLevSql).map { row =>
      if (row.isNullAt(0) | row.isNullAt(1)) null
      else (row.getLong(0),  row.getString(1),
        if (row.isNullAt(2)) null else row.getLong(2) , if (row.isNullAt(3)) null else row.getLong(3) , if (row.isNullAt(4)) null else  row.getLong(4))
    }

    val invalidCateLevNum = categoryLevRawRDD.filter(_ == null)
    val categoryLevRDD = categoryLevRawRDD.filter( _ != null).map(s => (s._1, (s._2, s._3, s._4, s._5)))
    logger.info("Invalid category level number count:  " + invalidCateLevNum.count())
    logger.info("validate category level number count:  " + categoryLevRDD.count())
    //  ##############   商品类别的  basic_info    #############
    //  加载商品数据
    val categoryRawRDD = hiveContext.sql(categorySql2).map { row =>
      if (row.anyNull) null else (row.getLong(0), row.getString(1), row.getString(2), row.getInt(3), row.getDouble(4))
     }.distinct()
    val invalidCateNum = categoryRawRDD.filter(_ == null)
    val validateCateRDD = categoryRawRDD.filter( _ != null)
    logger.info("Invalid goods record number count:  " + invalidCateNum.count())
    logger.info("Validate goods record number count: " + validateCateRDD.count())

    val subCategoryRDD = validateCateRDD.map { case (categoryId, productId, brandId, saleStatus, salePrice) =>
      (categoryId, Seq((productId, brandId, saleStatus, salePrice)))
    }.reduceByKey(_ ++ _).map { case (category, goods) =>
        val brandNum = goods.map(_._2).distinct.length  // 品牌数
        val inSaleNum = goods.filter(_._3 == 4).distinct.length  // 可售商品数
        val productNum = goods.map(_._1).distinct.length  // 产品数
        val goodsNum = goods.length  //  商品数
        val sortedPrices = goods.map(_._4).sorted
        val minPrice = sortedPrices.head  // min price
        val maxPrice = sortedPrices.last  // max price
        val middlePrice = sortedPrices(goodsNum / 2)  // middle price
        val averagePrice = sortedPrices.sum / goodsNum  // average price

        val lowLevProportion = sortedPrices.filter(_ < (maxPrice + 2 * minPrice) / 3).length.toDouble / goodsNum
        val midLevProportion = sortedPrices.filter(s => s < (2 * maxPrice + minPrice) / 3 && s > (maxPrice + 2 * minPrice) / 3 ).length.toDouble / goodsNum
        val highLevProportion = sortedPrices.filter(_ >= (2 * maxPrice + minPrice) / 3).length.toDouble / goodsNum

      (category, (brandNum, inSaleNum, productNum, minPrice, maxPrice, middlePrice, averagePrice, lowLevProportion, midLevProportion, highLevProportion))
    }

    val categoryStatisticRDD = subCategoryRDD.join(categoryLevRDD).map { case (category, (data, (categoryName, l1, l2, l3))) =>
        if (l3 != null && (!l3.equals(category))) (l3.asInstanceOf[Long], (categoryName, category), data)
        else if (l2 != null && (!l2.equals(category))) (l2.asInstanceOf[Long], (categoryName, category), data)
        else if (l1 != null) (l1.asInstanceOf[Long], (categoryName, category), data)
        else (Long.MaxValue, (category.toString, category), null)
    }

    val invalidRDD = categoryStatisticRDD.filter(s=>s._1 == Long.MaxValue && s._3 == null)
    logger.info("invalid result count is :  " + invalidRDD.count)

    val familyBytes = Bytes.toBytes("basic_info")
    val categoryNameBytes = Bytes.toBytes("category_name")
    val parentBytes = Bytes.toBytes("parent_category")
    val brandNumBytes = Bytes.toBytes("brand_num")
    val brandProportionBytes = Bytes.toBytes("proportion_of_brand")
    val goodsSumBytes = Bytes.toBytes("goods_num")
    val goodsProportionBytes = Bytes.toBytes("proportion_of_goods")
    val productBytes = Bytes.toBytes("product_num")
    val productProportionBytes = Bytes.toBytes("proportion_of_product")
    val lowPriceBytes = Bytes.toBytes("lowest price")
    val middlePriceBytes = Bytes.toBytes("middle_price")
    val highPriceBytes = Bytes.toBytes("highest_price")
    val averPriceBytes = Bytes.toBytes("average_price")
    val lowPriceProportionBytes = Bytes.toBytes("proportion_of_low_priced_goods")
    val midPriceProportionBytes = Bytes.toBytes("proportion_of_middle_priced_goods")
    val highPriceProportionBytes = Bytes.toBytes("proportion_of_high_priced_goods")

    val validateResultRDD = categoryStatisticRDD.filter(s=>s._1 != Double.MaxValue  && s._3 != null).map { case (parentCate, categoryName, data) => (parentCate, Seq((categoryName, data)))}
                            .reduceByKey(_ ++ _).map { case (parentCate, subCategoryData) =>
        val brandSum = subCategoryData.map(_._2).map(_._1).sum
        val goodsInSaleSum = subCategoryData.map(_._2).map(_._2).sum
        val productSum = subCategoryData.map(_._2).map(_._3).sum
        subCategoryData.map { s =>
          val put = new Put(Bytes.toBytes(s._1._2.toString))
          put.addColumn(familyBytes, categoryNameBytes, Bytes.toBytes(s._1._1))
          put.addColumn(familyBytes, parentBytes, Bytes.toBytes(parentCate))
          put.addColumn(familyBytes, brandNumBytes, Bytes.toBytes(s._2._1))
          put.addColumn(familyBytes, brandProportionBytes, Bytes.toBytes(s._2._1.toDouble / brandSum))
          put.addColumn(familyBytes, goodsSumBytes, Bytes.toBytes(s._2._2))
          put.addColumn(familyBytes, goodsProportionBytes, Bytes.toBytes(s._2._2.toDouble / goodsInSaleSum))
          put.addColumn(familyBytes, productBytes, Bytes.toBytes(s._2._3))
          put.addColumn(familyBytes, productProportionBytes, Bytes.toBytes(s._2._3.toDouble / productSum))
          put.addColumn(familyBytes, lowPriceBytes, Bytes.toBytes(s._2._4))
          put.addColumn(familyBytes, highPriceBytes, Bytes.toBytes(s._2._5))
          put.addColumn(familyBytes, middlePriceBytes, Bytes.toBytes(s._2._6))
          put.addColumn(familyBytes, averPriceBytes, Bytes.toBytes(s._2._7))
          put.addColumn(familyBytes, lowPriceProportionBytes, Bytes.toBytes(s._2._8))
          put.addColumn(familyBytes, midPriceProportionBytes, Bytes.toBytes(s._2._9))
          put.addColumn(familyBytes, highPriceProportionBytes, Bytes.toBytes(s._2._10))
          (new ImmutableBytesWritable(Bytes.toBytes(s._1._2)), put)
        }
    }.flatMap(s => s)

    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(TableOutputFormat.OUTPUT_TABLE, "category_analysis")
    hBaseConf.set("hbase.zookeeper.quorum", "slave23.bl.bigdata,slave24.bl.bigdata,slave21.bl.bigdata,slave22.bl.bigdata,slave25.bl.bigdata")
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    val job = Job.getInstance(hBaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[Put]])

    // save result to hbase
    validateResultRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)



    // ##############   traffic   ####################
    val now = System.currentTimeMillis()
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(now)
    cal.add(Calendar.DAY_OF_MONTH, -10)
    val day10 = getDateString(cal)
    cal.add(Calendar.DATE, -10)
    val day20 = getDateString(cal)
    cal.add(Calendar.DATE, -10)
    val day30 = getDateString(cal)

    val userBehaviorSql = "select member_id, goods_sid,  category_sid, event_date, behavior_type  " +
      s" from  recommendation.user_behavior_raw_data  where event_date >= '$day30'  "

    val userBehaviorRawRDD = hiveContext.sql(userBehaviorSql).map { row =>
      if (row.anyNull) null else (row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4))
    }
    userBehaviorRawRDD.cache()
    val invalidBehaviorRDD = userBehaviorRawRDD.filter(_ == null)
    logger.info(s"read in records number:  ${userBehaviorRawRDD.count()} ,  invalid user behavior raw data:  " + invalidBehaviorRDD.count)

    val trafficFamilyBytes = Bytes.toBytes("traffic")
    val PV30Bytes = Bytes.toBytes("last_30day_goods_pv")
    val pV10st = Bytes.toBytes("1st_10day_pv")
    val pV20st = Bytes.toBytes("2st_10day_pv")
    val pV30st = Bytes.toBytes("3st_10day_pv")

    val PV30RDD = userBehaviorRawRDD.filter(_ != null).filter(_._5 == "1000").map(s => ((s._3, if (s._4 < day20) 30 else if (s._4 < day10) 20 else 10 ),   1)).reduceByKey(_ + _)
    val pvStage = PV30RDD.map{ case (category, count) =>
          val put = new Put(Bytes.toBytes(category._1))
          if (category._2 == 10) put.addColumn(trafficFamilyBytes, pV10st, Bytes.toBytes(count))
          else if (category._2 == 20) put.addColumn(trafficFamilyBytes, pV20st, Bytes.toBytes(count))
          else put.addColumn(trafficFamilyBytes, pV30st, Bytes.toBytes(count))
      (new ImmutableBytesWritable(Bytes.toBytes(category._1)), put)
      }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    val pvSum = PV30RDD.map(s => (s._1._1, s._2)).reduceByKey(_ + _).map { case (category, count) =>
        val put = new Put(Bytes.toBytes(category))
        put.addColumn(trafficFamilyBytes, PV30Bytes, Bytes.toBytes(count))
      (new ImmutableBytesWritable(Bytes.toBytes(category)), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    val uv30Bytes = Bytes.toBytes("last_30day_goods_uv")
    val uv10stBytes = Bytes.toBytes("1st_10day_uv")
    val uv20stBytes = Bytes.toBytes("2nd_10day_uv")
    val uv30stBytes = Bytes.toBytes("3rd_10day_uv")

    val uv30RDD = userBehaviorRawRDD.filter(_ != null).filter(_._5 == "1000").map( s => (s._3, s._1)).distinct().map(s =>(s._1, 1)).reduceByKey(_ + _)
      .map { case (category, count) =>
          val put = new Put(Bytes.toBytes(category))
          put.addColumn(trafficFamilyBytes, uv30Bytes, Bytes.toBytes(count))
        (new ImmutableBytesWritable(Bytes.toBytes(category)), put)
      }
      uv30RDD.saveAsNewAPIHadoopDataset(job.getConfiguration)

    val uvRDD = userBehaviorRawRDD.filter(_!=null).filter(_._5 == "1000").map(s => ((s._3, if (s._4 < day20) 30 else if (s._4 < day10) 20 else 10), 1)).reduceByKey(_ + _)
      .map { case (category, count) =>
          val put = new Put(Bytes.toBytes(category._1))
          if (category._2 == 10) put.addColumn(trafficFamilyBytes, uv10stBytes, Bytes.toBytes(count))
          else if (category._2 == 20) put.addColumn(trafficFamilyBytes, uv20stBytes, Bytes.toBytes(count))
          else if (category._2 == 30) put.addColumn(trafficFamilyBytes, uv30stBytes, Bytes.toBytes(count))
        (new ImmutableBytesWritable(Bytes.toBytes(category._1)), put)
      }
      uvRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)

    val goodsRDD = userBehaviorRawRDD.filter(_ !=null).filter(_._5 == "1000").map(s => (s._3, s._1, s._2))

    goodsRDD.map(s => (s, 1)).reduceByKey(_ + _).map(s => (s._1._1, Seq((s._1._3, s._2)))).reduceByKey(_ ++ _)
      .map { case (category, goods) =>
          val put = new Put((Bytes.toBytes(category)))
          val select = goods.sortWith(_._2 > _._2).take(10).mkString("#")
          put.addColumn(trafficFamilyBytes, Bytes.toBytes("highest_uv_goods"), Bytes.toBytes(select))
        (new ImmutableBytesWritable(Bytes.toBytes(category)), put)
      }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    goodsRDD.map(s => ((s._1, s._3), 1)).reduceByKey(_ + _).map(s => (s._1._1, Seq((s._1._2, s._2)))).reduceByKey(_ ++ _).map { case (category, goods) =>
        val put = new Put(Bytes.toBytes(category))
      val select = goods.sortWith(_._2 > _._2).take(10).mkString("#")
        put.addColumn(trafficFamilyBytes, Bytes.toBytes("hightest_pv_goods"), Bytes.toBytes(select))
      (new ImmutableBytesWritable(Bytes.toBytes(category)), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)


   val brandSql = "select u.category_sid, u.member_id, g.brand_sid from recommendation.user_behavior_raw_data u " +
     s"inner join ( select distinct brand_sid, sid from recommendation.goods_avaialbe_for_sale_channel ) g on g.sid = u.goods_sid  " +
     s" where u.event_date > '${day30}' and u.member_id is not null and u.behavior_type = '1000'  "

    val brandRDD = hiveContext.sql(brandSql).map(row => if (row.anyNull) null else (row.getString(0), row.getString(1), row.getString(2))).filter(_ !=null)
    brandRDD.map(s => ((s._1, s._3), 1)).reduceByKey(_ + _).map(s=> (s._1._1, Seq((s._1._2, s._2)))).reduceByKey(_ ++ _)
    .map { case (category, brands) =>
          val put = new Put(Bytes.toBytes(category))
          val select = brands.sortWith(_._2 > _._2).take(10).mkString("#")
          put.addColumn(trafficFamilyBytes, Bytes.toBytes("hightest_pv_brand"), Bytes.toBytes(select))
        (new ImmutableBytesWritable(Bytes.toBytes(category)), put)
      }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    brandRDD.distinct().map(s => ((s._1, s._3), 1)).reduceByKey(_ + _).map(s=> (s._1._1, Seq((s._1._2, s._2)))).reduceByKey(_ ++ _)
      .map { case (category, brands) =>
        val put = new Put(Bytes.toBytes(category))
        val select = brands.sortWith(_._2 > _._2).take(10).mkString("#")
        put.addColumn(trafficFamilyBytes, Bytes.toBytes("highest_uv_brand"), Bytes.toBytes(select))
        (new io.ImmutableBytesWritable(Bytes.toBytes(category)), put)
      }.saveAsNewAPIHadoopDataset(job.getConfiguration)


    val hBaseConf2 = HBaseConfiguration.create()
    hBaseConf2.set(TableInputFormat.INPUT_TABLE, "category_analysis")
    hBaseConf2.set("hbase.zookeeper.quorum", "slave23.bl.bigdata,slave24.bl.bigdata,slave21.bl.bigdata,slave22.bl.bigdata,slave25.bl.bigdata")
    hBaseConf2.set("hbase.zookeeper.property.clientPort", "2181")
    val categoryPriceRDD = hiveContext.sparkContext.newAPIHadoopRDD(hBaseConf2, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2)
      .map { result=>
        val category = Bytes.toString(result.getRow)
        val lowPrice = result.getValue(familyBytes, lowPriceBytes)
        val highPrice = result.getValue(familyBytes, highPriceBytes)
        if (lowPrice == null | highPrice == null) null else (category, (Bytes.toDouble(lowPrice), Bytes.toDouble(highPrice)))
      }.filter(_ != null)
    logger.info("category price count: " + categoryPriceRDD.count())
    logger.info(s"hbase table 'category_analysis' :  \n${categoryPriceRDD.take(20).mkString("\n")} ")


    val browserGoodsAndPriceSql = " select u.member_id, u.goods_sid, u.category_sid, g.sale_price " +
      " from recommendation.user_behavior_raw_data u " +
      " inner join recommendation.goods_avaialbe_for_sale_channel g on g.sid = u.goods_sid and u.behavior_type = '4000' " +
      s" where u.behavior_type > '${day30}' and ( u.member_id <> 'null'  and u.member_id <> 'NULL' and u.member_id is not null ) "

    val browGoodsPriceRDD = hiveContext.sql(browserGoodsAndPriceSql)
      .map(row => (if (row.anyNull) null else (row.getString(2), (row.getString(0), row.getString(1), row.getDouble(3))))).filter(_ != null).distinct()

    val tmpp = browGoodsPriceRDD.join(categoryPriceRDD).map { case (category, ((memberId, goodsSid, salePrice), (lowPrice, high_price))) =>
        val delt = (high_price - lowPrice) / 3
        if (salePrice < (lowPrice + delt)) (category, (1, 0, 0))
        else if (salePrice < (lowPrice + 2 * delt)) (category, (0, 1, 0))
        else (category, (0, 0, 1))
    }.reduceByKey((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2, s1._3 + s2._3))

    tmpp.map { case (category, (lowNum, middNum, highNum)) =>
        val put = new Put(Bytes.toBytes(category))
        put.addColumn(trafficFamilyBytes, Bytes.toBytes("last_30day_high_price_band_uv"), Bytes.toBytes(highNum))
        put.addColumn(trafficFamilyBytes, Bytes.toBytes("last_30day_middle_price_band_uv"), Bytes.toBytes(middNum))
        put.addColumn(trafficFamilyBytes, Bytes.toBytes("last_30day_low_price_band_uv"), Bytes.toBytes(lowNum))
      (new io.ImmutableBytesWritable(Bytes.toBytes("")), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)


    // ###############           sales_status       ###########################

    val saleStatusFamilyBytes = Bytes.toBytes("sales_status")
    val saleMoney30Bytes = Bytes.toBytes("last_30day_sales")
    val saleAmt30Bytes = Bytes.toBytes("last_30day_sale_amount")
    val customerSumBytes = Bytes.toBytes("last_30day_number_of_customer")
    val brandSumBytes = Bytes.toBytes("brand_num")



    val saleSql = " select category_id ,  sum(sale_price), sum(sale_sum), count(distinct member_id), count(distinct brand_sid) from recommendation.order_info " +
                    s" where dt > ${day30.replace("-", "")}  and ORDER_STATUS NOT IN ('1001', '1029', '1100') group by category_id  "
    val saleRDD = hiveContext.sql(saleSql).map { row => if (row.anyNull) null
                  else (row.getLong(0).toString, row.getDouble(1), row.getDouble(2).toInt, row.getLong(3).toInt, row.getLong(4).toInt)}

    saleRDD.map { case (category, salePriceSum, saleNumSum, memberCount, brandCount) =>
        val put = new Put(Bytes.toBytes(category))
        put.addColumn(saleStatusFamilyBytes, saleMoney30Bytes, Bytes.toBytes(salePriceSum))
        put.addColumn(saleStatusFamilyBytes, saleAmt30Bytes, Bytes.toBytes(saleNumSum))
        put.addColumn(saleStatusFamilyBytes, customerSumBytes, Bytes.toBytes(memberCount))
        put.addColumn(saleStatusFamilyBytes, brandSumBytes, Bytes.toBytes(brandCount))
      (new ImmutableBytesWritable(Bytes.toBytes(category)), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    val saleRecordSql =  " select category_id , sale_price, sale_sum  " +
      s" from recommendation.order_info  where dt > ${day30.replace("-", "")}  and ORDER_STATUS NOT IN ('1001', '1029', '1100')  "
    val saleRDD2 = hiveContext.sql(saleRecordSql).map(row => if (row.anyNull) null
    else (row.getLong(0).toString, (row.getDouble(1), row.getDouble(2).toInt)))

    val tmp = saleRDD2.join(categoryPriceRDD).map { case (category, ((price, num), prices)) =>
      val delt = (prices._2 - prices._1) / 3
      if (price < (delt  + prices._1) ) (category, (price, 0.0, 0.0, num, 0, 0))
      else if ( (price >=  (delt + prices._1)) && (price < (2 * delt  + prices._1))) (category, (0.0, price, 0.0, 0, num, 0))
      else (category, (0.0,0.0, price, 0, 0, num))
    }.reduceByKey((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2, s1._3 + s2._3, s1._4 + s2._4, s1._5 + s2._5, s1._6 + s2._6))

    logger.info("category price :  \n" + tmp.collect().mkString("\n"))
//      .map { case (category, data) => (category, Seq(data))}.reduceByKey(_ ++ _)
      tmp.map { case (cate, goods) =>
          val sumPrice = goods._1 + goods._2 + goods._3
          val sum3 = goods._4 + goods._5 + goods._6

          val put = new Put(Bytes.toBytes(cate))
          put.addColumn(saleStatusFamilyBytes, Bytes.toBytes("percent_of_low_price_sales"), Bytes.toBytes(goods._1 / sumPrice))
          put.addColumn(saleStatusFamilyBytes, Bytes.toBytes("percent_of_middle_price_sales"), Bytes.toBytes(goods._2 / sumPrice))
          put.addColumn(saleStatusFamilyBytes, Bytes.toBytes("percent_of_high_price_sales"), Bytes.toBytes(goods._3 / sumPrice))

          put.addColumn(saleStatusFamilyBytes, Bytes.toBytes("percent_of_low_price_sale_amount"), Bytes.toBytes(goods._4.toDouble / sum3))
          put.addColumn(saleStatusFamilyBytes, Bytes.toBytes("percent_of_middle_price_sale_amount"), Bytes.toBytes(goods._5.toDouble / sum3))
          put.addColumn(saleStatusFamilyBytes, Bytes.toBytes("percent_of_high_price_sale_amount"), Bytes.toBytes(goods._6.toDouble / sum3))
        (new ImmutableBytesWritable(Bytes.toBytes(cate)), put)
      }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    val highestSql = s" select category_id, goods_sid, brand_sid, sale_price, sale_sum  from recommendation.order_info  " +
      s" where dt > ${day30.replace("-", "")}  and ORDER_STATUS NOT IN ('1001', '1029', '1100')  "

    val highestRDD = hiveContext.sql(highestSql).map(row => if (row.anyNull) null
    else (row.getLong(0).toString, row.getString(1), row.getString(2), row.getDouble(3), row.getDouble(4).toInt))

    highestRDD.map(s=> ((s._1, s._2), (s._4, s._5))).reduceByKey((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2  ))
      .map { case (cate, data) =>(cate._1, Seq((cate._2, data)))}.reduceByKey(_ ++ _).map { case (cate, goods) =>
        val money = goods.map(s=> (s._1, s._2._1)).sortWith(_._2 > _._2).take(10).mkString("#")
        val n = goods.map(s => (s._1, s._2._2)).sortWith(_._2 > _._2).take(10).mkString("#")
        val put = new Put(Bytes.toBytes(cate))
        put.addColumn(saleStatusFamilyBytes, Bytes.toBytes("highest_sale_amount_goods"), Bytes.toBytes(n))
        put.addColumn(saleStatusFamilyBytes, Bytes.toBytes("highest_sales_goods"), Bytes.toBytes(money))
      (new ImmutableBytesWritable(Bytes.toBytes(cate)), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    highestRDD.map(s=> ((s._1, s._3), (s._4, s._5))).reduceByKey((s1, s2) => (s1._1 + s2._1, s1._2 + s2._2  ))
      .map { case (cate, data) =>(cate._1, Seq((cate._2, data)))}.reduceByKey(_ ++ _).map { case (cate, goods) =>
      val money = goods.map(s=> (s._1, s._2._1)).sortWith(_._2 > _._2).take(10).mkString("#")
      val n = goods.map(s => (s._1, s._2._2)).sortWith(_._2 > _._2).take(10).mkString("#")
      val put = new Put(Bytes.toBytes(cate))
      put.addColumn(saleStatusFamilyBytes, Bytes.toBytes("highest_sale_amount_brand"), Bytes.toBytes(n))
      put.addColumn(saleStatusFamilyBytes, Bytes.toBytes("highest_sales_brand"), Bytes.toBytes(money))
      (new ImmutableBytesWritable(Bytes.toBytes(cate)), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)


    println("  ####    END     #####")


  }

  /** get date string */
  def getDateString(cal: Calendar): String = {
    val month = cal.get(Calendar.MONTH) + 1
    val day = cal.get(Calendar.DATE)
    cal.get(Calendar.YEAR) + "-" + (if (month < 10) "0" + month else month) + "-" + (if (day < 10) "0" + day else day)
  }

}
