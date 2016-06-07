package com.bl.bigdata.useranalyze

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.util.{ConfigurationBL, SparkFactory, Tool}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.json.{JSONArray, JSONObject}

/**
*  Created by MK33 on 2016/4/25.
*/
class UserGoodsWeight extends Tool
{

  override def run(args: Array[String]): Unit =
  {

    logger.info("======  persona 开始计算........  =======")

    val sc = SparkFactory.getSparkContext("user-goods-weight")
    val sql = "select u.member_id, g.category_id, g.category_name, g.brand_sid, u.behavior_type  " +
      " from recommendation.user_behavior_raw_data u  " +
      " left join recommendation.goods_avaialbe_for_sale_channel g on u.goods_sid = g.sid " +
      " where isnotnull(u.member_id) and isnotnull(g.category_id) and isnotnull(g.brand_sid) and " +
      " ( u.behavior_type = 1000 or u.behavior_type = 2000 or u.behavior_type = 3000 or u.behavior_type = 4000 )"

    val userBehaviorSql = ConfigurationBL.get("user.behavior.data.sql")
    logger.info(s"sql: $userBehaviorSql ")

    val rawRDD = ReadData.readHive(sc, userBehaviorSql)
    val trainRDD = rawRDD.map { case Array(cookie, category, categoryName, brand, behavior) =>
                              (cookie, category, categoryName, brand, behavior) }
                         .map(s => (s._1, s._2, s._3, s._4, getRating(s._5)))
    trainRDD.cache()
    // 每个类别，用户浏览的最大次数
    val categoryMap = trainRDD.map { case (cookie, category, categoryName, brand, rating) => ((cookie, category), 1L) }
                              .reduceByKey(_ + _)
                              .map(s => (s._1._2, s._2))
                              .reduceByKey(Math.max).collectAsMap()
    logger.debug("category count: " + categoryMap.size)
    // 每个品牌，用户浏览的最大次数
    val brandMap = trainRDD.map(s => ((s._1, s._2, s._4), 1L))
                           .reduceByKey(_ + _)
                           .map(s => ((s._1._2, s._1._3), s._2))
                           .reduceByKey(Math.max)
                           .collectAsMap()
    logger.debug("brand count: " + brandMap.size)

    val categoryBroadcast = sc.broadcast(categoryMap)
    val brandBroadcast = sc.broadcast(brandMap)

    // 每个用户浏览不同品牌的次数的权重
    val userGoodsCount = trainRDD.map(s => ((s._1, s._2, s._3, s._4), s._5)).reduceByKey(_ + _)
    // 计算类别权重
    val categoryWeightRDD = userGoodsCount.map { case ((cookie, category, categoryName, brand), rating) => ((cookie, category, categoryName), rating) }
                                          .reduceByKey(_ + _)
                                          .map { case ((cookie, category, categoryName), rating) =>
                                            val categoryMap = categoryBroadcast.value
                                            val divider = categoryMap(category)
                                            val categoryWeight = if (divider == 1) 1.0 else Math.log(rating) / Math.log(divider)
                                            ((cookie, category), (categoryName, categoryWeight))
                                          }
    // 计算品牌权重
    val brandWeightRDD = userGoodsCount.map { case ((cookie, category, categoryName, brand), rating) =>
                                              val brandMap = brandBroadcast.value
                                              val divider = brandMap((category, brand))
                                              val brandWeight = if (divider == 1) 1.0 else Math.log(rating) / Math.log(divider)
                                              ((cookie, category), Seq((brand, brandWeight)))
                                            }
                                      .reduceByKey(_ ++ _)
                                      .mapValues(_.sortWith(_._2 > _._2))

    val pricePreferenceRDD = getPricePreference

    val r0 = categoryWeightRDD.join(brandWeightRDD).join(pricePreferenceRDD)
            .map { case ((cookie, category), ((categoryWeight, brands), price)) => (cookie, Seq((category, categoryWeight, brands, price))) }
            .reduceByKey(_ ++ _)

    val hbaseConf = HBaseConfiguration.create()
    val table = ConfigurationBL.get("user.goods.weight.table")
    val columnFamily = ConfigurationBL.get("user.goods.weight.table.column.family")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = Job.getInstance(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])
    job.setOutputFormatClass(classOf[TableOutputFormat[Put]])
    val columnFamilyBytes = Bytes.toBytes(columnFamily)
    val column = Bytes.toBytes("cbp")

    r0.map { case (cookie, categoryInfo) =>
        val put = new Put(Bytes.toBytes(cookie))
        val jsonArray = new JSONArray()

        categoryInfo.foreach { case (category, categoryWeight, brands, price) =>
            val json = new JSONObject()
            json.put("category_sid", category)
            json.put("category_name", categoryWeight._1)
            json.put("doi", (categoryWeight._2 * 1000).asInstanceOf[Int].toDouble / 1000 toString )

            val jsonPrice = new JSONObject()
            jsonPrice.put(price._4._1 + "-" + price._4._2, price._1)
            jsonPrice.put(price._4._2 + "-" + price._4._3, price._2)
            jsonPrice.put(price._4._3 + "-" + price._4._4, price._3)

            json.put("price", jsonPrice)

            val jsonBrandArray = new JSONArray()
            for (band <- brands) {
              val jsonBrand = new JSONObject()
              jsonBrand.put("brand_sid", band._1)
//              try {
                jsonBrand.put("doi", (band._2 * 1000).asInstanceOf[Int].toDouble / 1000 toString)
//              } catch {
//                case e:JSONException => logger.error("json error: " + band._2)
//              }
              jsonBrandArray.put(jsonBrand)
            }
            json.put("band", jsonBrandArray)
            jsonArray.put(json)
        }
      val json = new JSONObject()
      json.put("data", jsonArray)
      put.addColumn(columnFamilyBytes, column, Bytes.toBytes(json.toString))
      (new ImmutableBytesWritable(Bytes.toBytes("")), put)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)

    trainRDD.unpersist()

    logger.info("======  persona 计算结束  =======")

  }


  def getRating(s: String): Int = {
    s match {
      case "1000" => 1
      case "2000" => 3
      case "3000" => 0
      case "4000" => 5
    }
  }


  /** 计算用户的价格偏好 */
  def getPricePreference: RDD[((String, String), (Double, Double, Double, (String, String, String, String)))] =
  {
    logger.debug("==== begin to calculator price preference ====")
    val sql = " select category_id, sid, sale_price from recommendation.goods_avaialbe_for_sale_channel where isnotnull(category_id)"

    val sqlGoods = ConfigurationBL.get("persona.sql.goods")
    val sqlBrowser = ConfigurationBL.get("persona.sql.browser")
    logger.info(sqlGoods)
    logger.info(sqlBrowser)
    val hiveContext = SparkFactory.getHiveContext

    val rawRDD = hiveContext.sql(sqlGoods).rdd.map( row => (row.getLong(0).toString, row.getString(1), row.getDouble(2))).distinct()
    logger.debug(s"价格偏好: ${rawRDD.count()}")
    // 计算商品所属类别的价格划分范围
    val out = calculate(rawRDD)
    out.cache()

    val browser = hiveContext.sql(sqlBrowser).rdd.map(row => (row.getString(0), (row.getString(1), row.getString(2), row.getDouble(3)))).distinct()
    val a = browser.join(out).map { case (category, ((cookie, goodsID, price), levels)) =>
      val level = if (price > levels._3) "H" else if (price > levels._2) "M" else "L"
      ((cookie, category, (levels._1.toString, levels._2.toString, levels._3.toString, levels._4.toString)), Seq(level))
    }
    // 用户对某个类别的价格偏好
    val pricePreference = a.reduceByKey(_ ++ _).map { s =>
      val low = s._2.count(_ == "L")
      val mid = s._2.count(_ == "M")
      val high = s._2.count(_ == "H")
      val size = s._2.length
      ((s._1._1, s._1._2), (low.toDouble / size, mid.toDouble / size, high.toDouble / size, s._1._3))
    }

    pricePreference
  }

  /**
   * @param data RDD[(商品类别, 商品id, 商品价格)]， 商品价格划分：0 - 40% 为低端(L), 40 - 80% 为中端(M), 80 - 100% 为高端(H)
   * @return 返回某个类别的商品价格价格段划分
   */
  def calculate(data: RDD[(String, String, Double)]): RDD[(String, (Double, Double, Double, Double))] = {
    val kv = data.map { case (category, goodsID, price) => (category, Seq((goodsID, price))) }
    val kvs = kv.reduceByKey(_ ++ _).map { s =>
      val size = s._2.length
      val sorted = s._2.sortWith(_._2 < _._2)
      val high = sorted(size * 8 / 10)._2
      val mid = sorted(size * 4 / 10)._2
      (s._1, (sorted.head._2, mid, high, sorted(size - 1)._2))
    }
    kvs
  }



}
