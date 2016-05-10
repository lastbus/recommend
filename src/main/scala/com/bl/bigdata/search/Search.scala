package com.bl.bigdata.search

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.util.{ConfigurationBL, SparkFactory, Tool}

/**
 * Created by MK33 on 2016/5/5.
 */
class Search  extends Tool
{

  override def run(args: Array[String]): Unit =
  {
    val sc = SparkFactory.getSparkContext("search")
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val now = new Date().getTime
    val before = new Date(now - 24000L * 3600)
    val date = ConfigurationBL.get("search.date", sdf.format(before))
    val hiveTable= ConfigurationBL.get("search.goods.save.table")

    val sqlSale = "select c.id, g.channel_id, g.goods_code, g.sale_cum_num, g.sale_cum_amount, g.index_type" +
                  " from  idmdata.m_sr_eg_goods_num_amount g " +
                  " inner join idmdata.dim_search_category c on c.goods_sid = g.goods_code and g.channel_id = c.channel_sid " +
                  " where g.cdate = " + date

    val rawData = ReadData.readHive(sc, sqlSale)
    val saleNum = rawData.map{ case Array(category, channelID, goodsCode, saleNum, saleAmount, index_type)
                                    => ((category, channelID, index_type), Seq((goodsCode, saleNum.toInt, saleAmount.toDouble))) }
                          .reduceByKey(_ ++ _)
                          .map { case ((category, channel, indexType), array) =>

                            val weight = if (indexType == "1") 15.0 else 7.5
                            val max = array.map(_._2).max
                            val min = array.map(_._2).min
                            val divide = if (max != min) max - min else 1

                            val weight2 = if (indexType == "1") 15.0 else 7.5
                            val max2 = array.map(_._3).max
                            val min2 = array.map(_._3).min
                            val divide2 = if (max2 != min2) max2 - min2 else 1

                            array.map(item => ((item._1, category, channel), weight * (item._2 - min) / divide + weight2 * (item._3 - min2) / divide2))
                          }.flatMap(s => s)

    val sqlVisit = "select c.id, pv.channel_id, pv.goods_code, pv.goods_pv, pv.goods_uv, pv.index_type  " +
                  " from idmdata.m_sr_eg_goods_pv_uv pv " +
                  " inner join idmdata.dim_search_category c on c.goods_sid = pv.goods_code  and pv.channel_id = c.channel_sid " +
                  "where pv.cdate = " + date
    val pvUVRawData = ReadData.readHive(sc, sqlVisit)
    val pv = pvUVRawData.map { case Array(category, channel, goodsID, pv, uv, indexType)
                                    => ((category, channel, indexType), Seq((goodsID, pv.toInt, uv.toInt))) }
                        .reduceByKey(_ ++ _)
                        .map { case ((cate, channel, index), array) =>

                                val weight = if (index == "1") 5.0 else 2.5
                                val max = array.map(_._2).max
                                val min = array.map(_._2).min
                                val divide = if (max != min) max - min else 1

                                val weight2 = if (index == "1") 15.0 else 7.5
                                val max2 = array.map(_._3).max
                                val min2 = array.map(_._3).min
                                val divide2 = if (max2 != min2) max2 - min2 else 1
                                array.map(item => ((item._1, cate, channel), weight * (item._2 - min) / divide + weight2 * (item._3 - min2) / divide2))
                              }.flatMap(s=>s)

    val hasChannel = saleNum.union(pv)


    val sqlEva = "select  c.id, c.channel_sid, ev.goods_code, ev.goods_eva_num, ev.index_type  " +
                  " from idmdata.m_sr_eg_goods_eva_num ev " +
                  " inner join idmdata.dim_search_category c on c.goods_sid = ev.goods_code " +
                  "where ev.cdate = " + date
    val evaRawData = ReadData.readHive(sc, sqlEva)
    val eva = evaRawData.map { case Array(category, channel, goodsID, num, indexType) =>
                                      ((category, channel, indexType), Seq((goodsID, num.toInt)))}
                        .reduceByKey(_ ++ _).map { case ((cate, channel, index), array) =>

                                val weight = if (index == "1") 5.0 else 2.5
                                val max = array.map(_._2).max
                                val min = array.map(_._2).min
                                val divide = if (max != min) max - min else 1

                                array.map(item => ((item._1, cate, channel), weight * (item._2 - min) / divide))
                        }.flatMap(s => s)

    val sqlEvaScore = "select  c.id, c.channel_sid, ev.goods_code, ev.goods_avg_score, ev.index_type  " +
                      " from idmdata.m_sr_eg_goods_eva_score ev " +
                      " inner join idmdata.dim_search_category c on c.goods_sid = ev.goods_code " +
                      "where ev.cdate = " + date

    val evaScoreRawData = ReadData.readHive(sc, sqlEvaScore)
    val evaScore = evaScoreRawData.map{ case Array(category, channel, goodsID, score, indexType) =>
                                                ((category, channel, indexType), Seq((goodsID, score.toDouble)))}
                                  .reduceByKey(_ ++ _).map { case ((cate, channel, index), array) =>

                                                val weight = if (index == "1") 5.0 else 2.5
                                                val max = array.map(_._2).max
                                                val min = array.map(_._2).min
                                                val divide = if (max != min) max - min else 1

                                                array.map(item => ((item._1, cate, channel),  weight * (item._2 - min) / divide))
                                              }.flatMap(s=>s)
    // 评价率
    val sqlEvaRatio = "select  c.id, c.channel_sid, ev.goods_code, ev.goods_eva_ratio, ev.index_type  " +
                      " from idmdata.m_sr_eg_goods_eva_ratio ev " +
                      " inner join idmdata.dim_search_category c on c.goods_sid = ev.goods_code " +
                      "where ev.cdate = " + date
    val evaRation = ReadData.readHive(sc, sqlEvaRatio)
                             .map{ case Array(category, channel, goodsID, ratio, indexType) =>
                                          ((category, channel, indexType), Seq((goodsID, ratio.toDouble)))}
                              .reduceByKey(_ ++ _).map { case ((category, channel, index), array) =>
                                          val max = array.map(_._2).max
                                          val min = array.map(_._2).min
                                          val divide = if (max != min) max - min else 1
                                          array.map(item => ((item._1, category, channel), 5.0 * (item._2 - min) / divide))
                                      }.flatMap(s=>s)

    val noChannel = eva.union(evaScore).union(evaRation)

    val hiveContext = SparkFactory.getHiveContext
    import hiveContext.implicits._
    val r = hasChannel.union(noChannel).reduceByKey(_ + _).coalesce(1).map{ case ((goodsID, category, channel), weight) =>
            Score(date, category, channel, goodsID, weight)}.toDF()

    r.registerTempTable("tmp")
    hiveContext.sql(s"insert overwrite table $hiveTable  partition(dt='$date') " +
      s"select cdate, category, channel_id, goods_code, score from tmp")

  }

  def  f(seq : Traversable[(String, Int)], weight: Double): Traversable[(String, Double)]  =
  {
    val max = seq.map(_._2).max
    val min = seq.map(_._2).min
    seq.map(item => (item._1, weight * (item._2 - min) / (max - min)))
  }

}
case class Score(cdate: String, category: String, channel_id: String, goods_code: String, score: Double)