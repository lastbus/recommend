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
import org.json.JSONObject

/**
 * Created by MK33 on 2016/5/16.
 */
class Persona extends Tool {

  override def run(args: Array[String]): Unit = {

    logger.info("persona 开始计算.....")
    val sql = " select category_id, sid, sale_price from recommendation.goods_avaialbe_for_sale_channel "

    val sqlBrowser = ConfigurationBL.get("persona.sql")
    val table = ConfigurationBL.get("user.goods.weight.table")
    val sc = SparkFactory.getSparkContext("persona")
    val hiveContext = SparkFactory.getHiveContext

    val rawRDD = hiveContext.sql(sql).rdd.map( row => (row.getLong(0).toString, row.getString(1), row.getDouble(2))).distinct()
    // 计算商品所属类别的价格划分范围
    val out = calculate(rawRDD)
    out.cache()
//    val out2 = out.map { case (goodsID, level, prices, category) => (goodsID, (level, prices)) }
    // category_id, cookie_id, goods_id, sale_price
    val browser = hiveContext.sql(sqlBrowser).rdd.map( row => (row.getString(0), (row.getString(1), row.getString(2), row.getDouble(3)))).distinct
//    val categoryLevel = out.map(s => (s._4, s._3)).collectAsMap()
//    val categoryBroadcast = sc.broadcast(categoryLevel)

    val a = browser.join(out).map { case (category, ((cookie, goodsID, price), levels)) =>
                                      val level = if (price > levels._3) "H" else if (price > levels._2) "M" else "L"
                                      ((category, cookie), Seq(level))
                                    }
    // 用户对某个类别的价格偏好
    val b = a.reduceByKey(_ ++ _).map { s =>
      val low = s._2.count(_ == "L")
      val mid = s._2.count(_ == "M")
      val high = s._2.count(_ == "H")
      val size = s._2.length
      (s._1, (low.toDouble / size, mid.toDouble / size, high.toDouble / size))
    }

    val categoryWeight = getCategoryWeight()
    val joinPrice = categoryWeight.join(b).map { case ((category, cookie), (weight, l)) => (category, (cookie, weight, l))}
    val joinCategory = joinPrice.join(out).map { case (category, ((cookie, weight, lev), levels)) => (cookie, Seq((category, weight, lev, levels)))}

    val json = joinCategory.reduceByKey(_ ++ _).map { case (cookie, categories) =>
      val jsonObject = new JSONObject()
      categories.map { case (cate, weight, lev, prices) =>
        val json = new JSONObject()
        json.put("weight", weight.toString)
        json.put("low", lev._1)
        json.put("low_price", prices._1 + "-" + prices._2)
        json.put("mid", lev._2)
        json.put("mid_price", prices._2 + "-" + prices._3)
        json.put("high", lev._3)
        json.put("high_price", prices._3 + "-" + prices._4)
        jsonObject.put(cate, json)
      }
      (cookie, jsonObject.toString)
    }

    //保存在 HBase 中。
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = Job.getInstance(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])
    job.setOutputFormatClass(classOf[TableOutputFormat[Put]])
    val columnFamily = Bytes.toBytes("value")
    val column = Bytes.toBytes("price")
    val result = json.map { case (cookie, json) =>
        val put = new Put(Bytes.toBytes(cookie))
        put.addColumn(columnFamily, column, Bytes.toBytes(json))
      (new ImmutableBytesWritable(Bytes.toBytes("")), put)
    }
    result.saveAsNewAPIHadoopDataset(job.getConfiguration)
    logger.info("persona 计算结束.")
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
      val high = sorted(size / 8)._2
      val mid = sorted(size / 4)._2
      (s._1, (sorted(0)._2, mid, high, sorted(size - 1)._2))
    }
    kvs
  }



  /** 用户类别权重 */
  def getCategoryWeight(): RDD[((String, String), Double)] = {
    val sql = " select cookie_id, category_sid, behavior_type  " +
      " from recommendation.user_behavior_raw_data "
    val sc = SparkFactory.getSparkContext
    val rawRDD = ReadData.readHive(sc, sql)
    val trainRDD = rawRDD.map { case Array(cookie, category, behavior) =>
      (cookie, category, behavior) }
      .filter(s => s._3 == "1000" | s._3 == "2000" | s._3 == "3000" | s._3 == "4000")
      .map(s => (s._1, s._2, getRating(s._3)))
    trainRDD.cache()
    // 每个类别，用户浏览的最大次数
    val categoryMap = trainRDD.map { case (cookie, category, rating) => ((cookie, category), 1L) }
      .reduceByKey(_ + _)
      .map(s => (s._1._2, s._2))
      .reduceByKey(Math.max).collectAsMap()
    val categoryBroadcast = sc.broadcast(categoryMap)

    // 计算类别权重
    val categoryWeightRDD = trainRDD.map { case (cookie, category, rating) => ((cookie, category), rating) }
      .reduceByKey(_ + _)
      .map { case ((cookie, category), rating) =>
        val categoryMap = categoryBroadcast.value
        val categoryWeight = Math.log(rating) / Math.log(categoryMap(category))
        ((category, cookie), categoryWeight)
      }
    trainRDD.unpersist()
    categoryWeightRDD
  }

  def getRating(s: String): Int = {
    s match {
      case "1000" => 1
      case "2000" => 3
      case "3000" => -2
      case "4000" => 5
    }
  }



}
