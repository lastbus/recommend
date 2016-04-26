package com.bl.bigdata.useranalyze

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.{Item, ReadData}
import com.bl.bigdata.util.{ConfigurationBL, SparkFactory, Tool}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job

/**
*  Created by MK33 on 2016/4/25.
*/
class UserGoodsWeight extends Tool {
  override def run(args: Array[String]): Unit = {
    val sc = SparkFactory.getSparkContext("user goods weight")
    val output = ConfigurationBL.get("recmd.output", "")
    val hbase = output.contains("hbase")
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val today = sdf.format(new Date())
    val sql = "select u.cookie_id, u.category_sid, g.brand_sid, u.behavior_type  " +
      " from recommendation.user_behavior_raw_data u  " +
      " left join recommendation.goods_avaialbe_for_sale_channel g on u.goods_sid = g.sid "

    val rawRDD = ReadData.readHive(sc, sql)
    val trainRDD = rawRDD.map{ case Item(Array(cookie, category, brand, behavior)) =>
      (cookie, category, brand, behavior)}
      .filter(s => s._4 == "1000" | s._4 == "2000" | s._4 == "3000" | s._4 == "4000")
    .map(s => (s._1, s._2, s._3, getRating(s._4)))
    //TODO 根据类别重新分区提高性能
    trainRDD.cache()
    // 每个类别，用户浏览的最大次数
    val categoryMap = trainRDD.map{ case (cookie, category, brand, rating) =>
      ((cookie, category), 1L)}
      .reduceByKey(_ + _).map(s => (s._1._2, s._2)).reduceByKey(Math.max).collectAsMap()
    // 每个品牌，用户浏览的最大次数
    val brandMap = trainRDD.map(s => ((s._1, s._2, s._3), 1L)).reduceByKey(_ + _)
      .map(s=> ((s._1._2, s._1._3), s._2))
      .reduceByKey(Math.max)
      .collectAsMap()

    val categoryBroadcast = sc.broadcast(categoryMap)
    val brandBroadcast = sc.broadcast(brandMap)

    // 每个用户浏览不同品牌的次数的权重
    val userGoodsCount = trainRDD.map(s => ((s._1, s._2, s._3), s._4))
      .reduceByKey(_ + _)
    // 计算类别权重
    val categoryWeightRDD = userGoodsCount.map { case ((cookie, category, brand), rating) => ((cookie, category), rating)}
    .reduceByKey(_ + _).map{ case ((cookie, category), rating) =>
      val categoryMap = categoryBroadcast.value
      val categoryWeight = Math.log(rating) / Math.log(categoryMap(category))
      ((cookie, category), categoryWeight)
    }
    // 计算品牌权重
    val brandWeightRDD = userGoodsCount.map { case ((cookie, category, brand), rating) =>
      val brandMap = brandBroadcast.value
      val brandWeight = Math.log(rating) / Math.log(brandMap((category, brand)))
      ((cookie, category), Seq((brand, brandWeight)))
    }.reduceByKey(_ ++ _).mapValues(_.sortWith(_._2 > _._2))
    // 将类别和其下的品牌权重联系起来
    val r = categoryWeightRDD.join(brandWeightRDD)
      .map{ case ((cookie, category), (categoryWeight, brands)) => (cookie, Seq((category, categoryWeight, brands)))
    }.reduceByKey(_ ++ _)

    if (hbase) {
      val hbaseConf = HBaseConfiguration.create()
      val table = ConfigurationBL.get("user.goods.weight.table")
      hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, table)
      val job = new Job(hbaseConf)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Put])
      job.setOutputFormatClass(classOf[TableOutputFormat[Put]])
      val columnFamily = Bytes.toBytes("value")
      // 准备输出到 HBase
      val r2 = r.map { case (cookie, cateBrand) =>
        val put = new Put(Bytes.toBytes(cookie))
        cateBrand.map{ case (category, categoryWeight, brands) =>
          val bString = category + ":" + brands.map(s => "("+s._1 + "," + s._2 + ")").mkString("#")
          put.addColumn(columnFamily, Bytes.toBytes(category), Bytes.toBytes(bString))
        }
        (new ImmutableBytesWritable(Bytes.toBytes("")), put)
      }

      r2.saveAsNewAPIHadoopDataset(job.getConfiguration)
    }
    trainRDD.unpersist()


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
