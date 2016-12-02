package com.bl.bigdata.recommend

import java.text.SimpleDateFormat
import java.util.Date
import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.util.{SparkFactory, Tool}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by YQ85 on 2016/5/25.
  */
class Search3 extends Tool {
  override def run(args: Array[String]): Unit = {

    val sc = SparkFactory.getSparkContext("search")
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val now = new Date().getTime
    val day = sdf.format(new Date(now - 24000L * 3600))
    val hiveContext = new HiveContext(sc)

    //销售额
    val sqlSale = "select c.id, g.goods_code, g.sale_cum_amount, g.index_type" +
      " from  idmdata.m_sr_eg_goods_num_amount g " +
      " inner join idmdata.dim_search_category c on c.goods_sid = g.goods_code " +
      s" where g.cdate = '$day' and channel_id IS NOT NULL"
    val rawData = ReadData.readHive(sc, sqlSale)
    val saleNum = rawData.map{ case Array(category, goodsCode, saleAmount, index_type)
    => ((category, index_type), Seq((goodsCode, saleAmount.toDouble))) }.reduceByKey(_ ++ _)
      .map { case ((category, indexType), array) =>
        val weight = if (indexType == "1") 20.0 else 15.0
        val array2 = array.groupBy(_._1).map(s => (s._1, s._2.map(_._2).sum))
        val max = array2.map(_._2).max
        val min = array2.map(_._2).min
        val divide = if (max != min) max - min else 1
        array2.map(item => ((item._1, category), weight * (item._2 - min) / divide))
      }.flatMap(s => s)

    //PV和UV
    val sqlVisit ="select c.id, pv.goods_code, pv.goods_pv, pv.goods_uv, pv.index_type  " +
      " from idmdata.m_sr_eg_goods_pv_uv pv " +
      " inner join idmdata.dim_search_category c on c.goods_sid = pv.goods_code " +
      s"where pv.cdate = '$day' and channel_id IS NOT NULL"
    val pvUVRawData = ReadData.readHive(sc, sqlVisit)
    val pv = pvUVRawData.map { case Array(category, goodsID, pv, uv, indexType)
    => ((category, indexType), Seq((goodsID, pv.toInt, uv.toInt))) }
      .reduceByKey(_ ++ _)
      .map { case ((cate, index), array) =>
        val array2 = array.groupBy(_._1).map(s => (s._1, s._2.map(_._2).sum, s._2.map(_._3).sum))
        val weight = if (index == "1") 4.0 else 2.0
        val max = array2.map(_._2).max
        val min = array2.map(_._2).min
        val divide = if (max != min) max - min else 1
        val weight2 = if (index == "1") 14.0 else 6.0
        val max2 = array2.map(_._3).max
        val min2 = array2.map(_._3).min
        val divide2 = if (max2 != min2) max2 - min2 else 1
        array2.map(item => ((item._1, cate), weight * (item._2 - min) / divide + weight2 * (item._3 - min2) / divide2))
      }.flatMap(s=>s)

    //转化率因子
    val sqlCR = "select num7.id, num7.goods_code, round(num7.sale_cum_num/uv7.goods_uv, 10)cr , num7.index_type from" +
      "(select c.id id , g.goods_code goods_code, g.sale_cum_num sale_cum_num, g.sale_cum_amount sale_cum_amount, g.index_type index_type from " +
      s"idmdata.m_sr_eg_goods_num_amount g inner join  idmdata.dim_search_category c on c.goods_sid = g.goods_code where g.index_type = '1' and g.cdate = '$day' and channel_id IS NOT NULL ) num7 " +
      " join (select c.id id, pv.goods_code goods_code, pv.goods_pv goods_pv, pv.goods_uv goods_uv, pv.index_type index_type from idmdata.m_sr_eg_goods_pv_uv pv " +
      s"inner join idmdata.dim_search_category c on c.goods_sid = pv.goods_code where pv.index_type = '1' and pv.cdate = '$day' and channel_id IS NOT NULL )uv7 " +
      "on (num7.goods_code = uv7.goods_code)"
    val crRawData = ReadData.readHive(sc, sqlCR)
    val crRdd = crRawData.map{ case Array(category, goodsID, cr, indexType)
    => (category, Seq((goodsID, cr.toDouble))) }
      .reduceByKey(_ ++ _)
      .map { case (cate, array) =>
        val array2 = array.groupBy(_._1).map(s => (s._1, s._2.map(_._2).sum))
        val weight =  15.0
        val max = array2.map(_._2).max
        val min = array2.map(_._2).min
        val divide = if (max != min) max - min else 1
        array2.map(item => ((item._1, cate), weight *(item._2 - min) / divide))
      }.flatMap(s=>s)


    val hasChannel = saleNum.union(pv).union(crRdd)

    //评价数
    val sqlEva = "select  c.id, ev.goods_code, ev.goods_eva_num, ev.index_type  " +
      " from idmdata.m_sr_eg_goods_eva_num ev " +
      " inner join idmdata.dim_search_category c on c.goods_sid = ev.goods_code " +
      s"where ev.cdate = '$day' and channel_sid IS NOT NULL"
    val evaRawData = ReadData.readHive(sc, sqlEva)
    val eva = evaRawData.map { case Array(category, goodsID, num, indexType) =>
      ((category, indexType), Seq((goodsID, num.toInt)))}
      .reduceByKey(_ ++ _).map { case ((cate, index), array) =>
      val array2 = array.groupBy(_._1).map(s => (s._1, s._2.map(_._2).sum))
      val weight = if (index == "1") 4.0 else 2.0
      val max = array2.map(_._2).max
      val min = array2.map(_._2).min
      val divide = if (max != min) max - min else 1
      array2.map(item => ((item._1, cate), weight * (item._2 - min) / divide))
    }.flatMap(s => s)

    //好评率
    val sqlEvaScore = "select  c.id, ev.goods_code, ev.goods_avg_score, ev.index_type  " +
      " from idmdata.m_sr_eg_goods_eva_score ev " +
      " inner join idmdata.dim_search_category c on c.goods_sid = ev.goods_code " +
      s"where ev.cdate = '$day' and channel_sid IS NOT NULL"
    val evaScoreRawData = ReadData.readHive(sc, sqlEvaScore)
    val evaScore = evaScoreRawData.map{ case Array(category, goodsID, score, indexType) =>
      (category, Seq((goodsID, score.toDouble)))}
      .reduceByKey(_ ++ _).map { case (cate, array) =>
      val array2 = array.groupBy(_._1).map(s => (s._1, s._2.map(_._2).sum))
      val max = array2.map(_._2).max
      val min = array2.map(_._2).min
      val divide = if (max != min) max - min else 1
      array2.map(item => ((item._1, cate),  10.0 * (item._2 - min) / divide))
    }.flatMap(s=>s)

    /*// 90天评价率
    val sqlEvaRatio = "select  c.id, c.channel_sid, ev.goods_code, ev.goods_eva_ratio, ev.index_type  " +
      " from idmdata.m_sr_eg_goods_eva_ratio ev " +
      " inner join idmdata.dim_search_category c on c.goods_sid = ev.goods_code " +
      "where ev.cdate = " + day
    val evaRation = ReadData.readHive(sc, sqlEvaRatio)
      .map{ case Array(category, channel, goodsID, ratio, indexType) =>
        ((category, channel, indexType), Seq((goodsID, ratio.toDouble)))}
      .reduceByKey(_ ++ _).map { case ((category, channel, index), array) =>
      val max = array.map(_._2).max
      val min = array.map(_._2).min
      val divide = if (max != min) max - min else 1
      array.map(item => ((item._1, category, channel), 4.0 * (item._2 - min) / divide))
    }.flatMap(s=>s)
    val noChannel = eva.union(evaScore).union(evaRation)*/

    //属性填充率
    val sqlKeyPropsCR = " SELECT c.goods_sid, c.id, w.no_key_fill_num, w.no_key_num, w.key_fill_num, w.key_num " +
      " FROM recommendation.m_sr_eg_goods_weight_key_fill_rate w  " +
      " JOIN idmdata.dim_search_category c ON c.goods_sid = w.goods_id   " +
      " where c.channel_sid is not null "
    val goodsRdd = hiveContext.sql(sqlKeyPropsCR).rdd.map(row => (row.getLong(0), row.getLong(1), row.getLong(2), row.getLong(3), row.getLong(4), row.getLong(5)))
    val KeyPropsCR = goodsRdd.map { case (goods_id, category_id, no_key_fill_num, no_key_num, key_fill_num, key_num) => {
      if (no_key_num == null || key_num == null || no_key_num == 0 || key_num == 0) {
        (goods_id, category_id, 0.0)
      }
      else {
        (goods_id, category_id, (no_key_fill_num.toDouble / no_key_num + key_fill_num.toDouble / key_num) / 2)
      }
    }
    }.map{case (goods_sid,category_id, cr) => (category_id.toString, Seq((goods_sid, cr)))}
      .reduceByKey(_++_).map { case (category_id, array) =>
      val max = array.map(_._2).max
      val min = array.map(_._2).min
      val divide = if (max != min) max - min else 1
      array.map { case  item => ((item._1.toString, category_id), 2.0 * (item._2 - min) / divide) }
    }.flatMap(s => s)

    val noChannel = eva.union(evaScore).union(KeyPropsCR)
    import hiveContext.implicits._
    val r = hasChannel.union(noChannel).reduceByKey(_ + _).leftOuterJoin(crRdd).
      map { case ((goodsID, category), (score, conversion_rate)) =>
        if(conversion_rate.isEmpty)
          Array(
            Score(day, category, "1", goodsID, score, 0.0),
            Score(day, category, "2", goodsID, score, 0.0),
            Score(day, category, "3", goodsID, score, 0.0))
        else
          Array(
            Score(day, category, "1", goodsID, score, conversion_rate.get),
            Score(day, category, "2", goodsID, score, conversion_rate.get),
            Score(day, category, "3", goodsID, score, conversion_rate.get))}.flatMap(s=>s).toDF().registerTempTable("table1")
    hiveContext.sql(s"insert overwrite table idmdata.m_sr_eg_goods_weight partition(dt='$day') select * from table1")
  }
}
case class Score2(cdate: String, category: String, channel_id: String, goods_code: String, score: Double, conversion_rate:Double)


