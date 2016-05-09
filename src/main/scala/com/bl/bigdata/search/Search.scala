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
    val date0 = new Date().getTime
    new Date(date0 - 24000L * 3600)
    val date = ConfigurationBL.get("search.date", sdf.format(new Date()))
    val hiveTable= ConfigurationBL.get("search.goods.save.table")

    val sqlSale = "select c.id, g.channel_id, g.goods_code, g.sale_cum_num, g.sale_cum_amount, g.index_type" +
                  " from  idmdata.m_sr_eg_goods_num_amount g " +
                  " inner join idmdata.dim_search_category c on c.goods_sid = g.goods_code " +
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

                            array.map(item => ((item._1, channel), weight * (item._2 - min) / divide + weight2 * (item._3 - min2) / divide2))
                          }.flatMap(s => s)

    val sqlVisit = "select c.id, pv.channel_id, pv.goods_code, pv.goods_pv, pv.goods_uv, pv.index_type  " +
                  " from idmdata.m_sr_eg_goods_pv_uv pv " +
                  " inner join idmdata.dim_search_category c on c.goods_sid = pv.goods_code " +
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
                                array.map(item => ((item._1, channel), weight * (item._2 - min) / divide + weight2 * (item._3 - min2) / divide2))
                              }.flatMap(s=>s)

    val hasChannel = saleNum.union(pv).reduceByKey(_ + _).map(s=> (s._1._1, (s._1._2, s._2)))

    val sqlEva = "select  c.id, ev.goods_code, ev.goods_eva_num, ev.index_type  " +
                  " from idmdata.m_sr_eg_goods_eva_num ev " +
                  " inner join idmdata.dim_search_category c on c.goods_sid = ev.goods_code " +
                  "where ev.cdate = " + date
    val evaRawData = ReadData.readHive(sc, sqlEva)
    val eva = evaRawData.map{ case Array(category, goodsID, num, indexType) =>
                                      ((category, indexType, goodsID), num.toInt)}.reduceByKey(_ + _)
                        .map(s => ((s._1._1, s._1._2), Seq((s._1._3, s._2))))
                        .reduceByKey(_ ++ _).map { case ((cate, index), array) =>
                                val weight = if (index == "1") 5.0 else 2.5
                                val max = array.map(_._2).max
                                val min = array.map(_._2).min
                                val divide = if (max != min) max - min else 1
                                array.map(item => (item._1, weight * (item._2 - min) / divide))
                        }.flatMap(s => s)
    val sqlEvaScore = "select  c.id, ev.goods_code, ev.goods_avg_score, ev.index_type  " +
                      " from idmdata.m_sr_eg_goods_eva_score ev " +
                      " inner join idmdata.dim_search_category c on c.goods_sid = ev.goods_code " +
                      "where ev.cdate = " + date
    val evaScoreRawData = ReadData.readHive(sc, sqlEvaScore)
    val evaScore = evaScoreRawData.map{ case Array(category, goodsID, score, indexType) =>
                                                ((category, indexType, goodsID), score.toInt)}.reduceByKey(_ + _)
                                  .map(s => ((s._1._1, s._1._2), Seq((s._1._3, s._2))))
                                  .reduceByKey(_ ++ _).map { case ((cate, index), array) =>
                                                val weight = if (index == "1") 5.0 else 2.5
                                                val max = array.map(_._2).max
                                                val min = array.map(_._2).min
                                                val divide = if (max != min) max - min else 1
                                                array.map(item => (item._1, weight * (item._2 - min) / divide))
                                              }.flatMap(s=>s)
    // 评价率
    val sqlEvaRatio = "select  c.id, ev.goods_code, ev.goods_eva_ratio, ev.index_type  " +
                      " from idmdata.m_sr_eg_goods_eva_ratio ev " +
                      " inner join idmdata.dim_search_category c on c.goods_sid = ev.goods_code " +
                      "where ev.cdate = " + date
    val evaRation = ReadData.readHive(sc, sqlEvaRatio)
                             .map{ case Array(category, goodsID, ratio, indexType) =>
                                          ((category, indexType, goodsID), ratio.toDouble)}
                              .reduceByKey(_ + _).map(s => ((s._1._1, s._1._2), Seq((s._1._3, s._2))))
                              .reduceByKey(_ ++ _).map { case ((category, index), array) =>
                                          val max = array.map(_._2).max
                                          val min = array.map(_._2).min
                                          val divide = if (max != min) max - min else 1
                                          array.map(item => (item._1, 5.0 * (item._2 - min) / divide))
                                      }.flatMap(s=>s)

    val noChannel = eva.union(evaScore).union(evaRation).reduceByKey(_ + _)

    val hiveContext = SparkFactory.getHiveContext
    import hiveContext.implicits._
    val r = hasChannel.join(noChannel).coalesce(1).map{ case (goodsID, ((channel, w1), w2)) =>
            Score(goodsID, date, channel, w1 + w2)}.toDF()

    r.registerTempTable("tmp")
    hiveContext.sql(s"insert into $hiveTable  partition(dt='$date') " +
      s"select goods_id, date, channel, score from tmp")

  }

  def spark2Hive(): Unit ={

//    val sc = SparkFactory.getSparkContext
//    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
//    hiveContext.setConf("spark.sql.hive.convertMetastoreParquet", "true")
//    case class Person(id:String, name:String)
//    val table = "cookie"
////    hiveContext.sql("use default")
//    val data = sc.makeRDD(1 to 10).map(s=>Person(s.toString, s.toString))
//    import hiveContext.implicits._
//    val h = data.toDF().registerTempTable("hh")
//
//    val df = data.toDF()
//    df
//    hiveContext.sql(s"insert into $table  select id, name from hh")

  }



  def  f(seq : Traversable[(String, Int)], weight: Double): Traversable[(String, Double)]  =
  {
    val max = seq.map(_._2).max
    val min = seq.map(_._2).min
    seq.map(item => (item._1, weight * (item._2 - min) / (max - min)))
  }

}

case class Score(goods_id: String, date: String, channel: String, score: Double)