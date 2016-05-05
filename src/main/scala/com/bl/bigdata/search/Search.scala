package com.bl.bigdata.search

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.{Item, ReadData}
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
    val date = ConfigurationBL.get("search.date", sdf.format(new Date()))

    val sqlSale = "select c.id, g.channel_id, g.goods_code, g.sale_cum_num, g.sale_cum_amount, g.index_type " +
                  " from  idmdata.m_sr_eg_goods_num_amount g " +
                  "inner join idmdata.dim_search_category c on c.goods_sid = g.goods_code " +
                  "where g.cdate = " + date


    val rawData = ReadData.readHive(sc, sqlSale)
    val saleNum = rawData.map{ case Item(Array(category, channelID, goodsCode, saleNum, saleAmount, index_type))
                            => ((category, index_type), Seq((goodsCode, saleNum.toInt))) }
                          .reduceByKey(_ ++ _)
                            .map { case ((category, indexType), array)
                            =>
                              val weight = if (indexType == "1") 15.0 else 7.5
                              val max = array.map(_._2).max
                              val min = array.map(_._2).min
                              val divide = if (max == min) max - min else 1
                            (category, array.map(item => (item._1, weight * (item._2 - min) / divide)))
                          }
    logger.info(saleNum.first())

    val saleMoney = rawData.map { case Item(Array(category, channelID, goodsCode, saleNum, saleAmount, index_type))
                                            => ((category, index_type), Seq((goodsCode, saleAmount.toDouble))) }
                            .reduceByKey(_ ++ _)
                            .map { case ((category, indexType), array) =>
                                val weight = if (indexType == "1") 15.0 else 7.5
                                val max = array.map(_._2).max
                                val min = array.map(_._2).min
                              val divide = if (max == min) max - min else 1
                                (category, array.map(item => (item._1, weight * (item._2 - min) / divide)))
                            }
    logger.info(saleMoney.first())
    val sqlVisit = "select c.id, pv.goods_code, pv.goods_pv, pv.goods_uv, pv.index_type  " +
                  " from idmdata.m_sr_eg_goods_pv_uv pv " +
                  " inner join idmdata.dim_search_category c on c.goods_sid = pv.goods_code " +
                  "where pv.cdate = " + date

    val pvUVRawData = ReadData.readHive(sc, sqlVisit)
    val pv = pvUVRawData.map { case Item(Array(category, goodsID, pv, uv, indexType)) =>
      ((category, indexType), Seq((goodsID, pv.toInt)))
    }.reduceByKey(_ ++ _).map { case ((cate, index), array) =>
      val weight = if (index == "1") 5.0 else 2.5
      val max = array.map(_._2).max
      val min = array.map(_._2).min
      val divide = if (max == min) max - min else 1
      (cate, array.map(item => (item._1, weight * (item._2 - min) /divide)))
    }
    logger.info(pv.first())
    val vu = pvUVRawData.map { case Item(Array(category, goodsID, pv, uv, indexType)) =>
      ((category, indexType), Seq((goodsID, uv.toInt)))
    }.reduceByKey(_ ++ _).map { case ((cate, index), array) =>
      val weight = if (index == "1") 15.0 else 7.5
      val max = array.map(_._2).max
      val min = array.map(_._2).min
      val divide = if (max == min) max - min else 1
      (cate, array.map(item => (item._1, weight * (item._2 - min) /divide)))
    }

    logger.info(vu.first())
    val sqlEva = "select  c.id, ev.goods_code, ev.goods_eva_num, ev.index_type  " +
                  " from m_sr_eg_goods_eva_num ev " +
                  " inner join dim_search_category c on c.goods_sid = ev.goods_code " +
                  "where ev.cdate = " + date

    val evaRawData = ReadData.readHive(sc, sqlEva)

    val eva = evaRawData.map{ case Item(Array(category, goodsID, num, indexType)) =>
                                      ((category, indexType), Seq((goodsID, num.toInt)))}
                        .reduceByKey(_ ++ _).map { case ((cate, index), array) =>
                                val weight = if (index == "1") 5.0 else 2.5
                                val max = array.map(_._2).max
                                val min = array.map(_._2).min
                                val divide = if (max == min) max - min else 1
                                (cate, array.map(item => (item._1, weight * (item._2 - min) /divide)))
                        }
    logger.info(eva.first())

    val sqlEvaScore = "select  c.id, ev.goods_code, ev.goods_avg_score, ev.index_type  " +
                      " from idmdata.m_sr_eg_goods_eva_score ev " +
                      " inner join idmdata.dim_search_category c on c.goods_sid = ev.goods_code " +
                      "where ev.cdate = " + date
    val evaScoreRawData = ReadData.readHive(sc, sqlEvaScore)
    val evaScore = evaScoreRawData.map{ case Item(Array(category, goodsID, score, indexType)) =>
                                                ((category, indexType), Seq((goodsID, score.toInt)))}
                                  .reduceByKey(_ ++ _).map { case ((cate, index), array) =>
                                  val weight = if (index == "1") 5.0 else 2.5
                                  val max = array.map(_._2).max
                                  val min = array.map(_._2).min
                                  val divide = if (max == min) max - min else 1
                                  (cate, array.map(item => (item._1, weight * (item._2 - min) /divide)))
                                }
    logger.info(evaScore.first())

    // 评价率
    val sqlEvaRatio = "select  c.id, ev.goods_code, ev.goods_avg_ratio, ev.index_type  " +
                      " from idmdata.m_sr_eg_goods_eva_ratio ev " +
                      " inner join idmdata.dim_search_category c on c.goods_sid = ev.goods_code " +
                      "where ev.cdate = " + date
    val evaRation = ReadData.readHive(sc, sqlEvaRatio)
                             .map{ case Item(Array(category, goodsID, ratio, indexType)) =>
                                          ((category, Seq((goodsID, ratio.toDouble))))}
                              .reduceByKey(_ ++ _).map { case (category, array) =>
                                val max = array.map(_._2).max
                                val min = array.map(_._2).min
                                val divide = if (max == min) max - min else 1
                                (category, array.map(item => (item._1, 5.0 * (item._2 - min) /divide)))
                              }

    logger.info(evaRation.first())


  }








  def  f(seq : Traversable[(String, Int)], weight: Double): Traversable[(String, Double)]  =
  {
    val max = seq.map(_._2).max
    val min = seq.map(_._2).min
    seq.map(item => (item._1, weight * (item._2 - min) / (max - min)))
  }

}
