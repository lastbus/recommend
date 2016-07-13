package com.bl.bigdata.search

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.util.{ConfigurationBL, RedisClient, SparkFactory, Tool}
import org.apache.commons.cli.{BasicParser, HelpFormatter, Option, Options}
import com.bl.bigdata.datasource.ReadData._

import scala.collection.mutable

/**
 * Created by MK33 on 2016/5/5.
 */
class Search  extends Tool
{

  override def run(args: Array[String]): Unit =
  {
    val optionMap = SearchCommandLineParser.parser(args)
    val sc = SparkFactory.getSparkContext("search")
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val now = new Date().getTime
    val before = new Date(now - 24000L * 3600)
//    val date = ConfigurationBL.get("search.date", sdf.format(before))
    val date = optionMap.getOrElse(SearchConf.day, sdf.format(before))
//    val hiveTable= ConfigurationBL.get("search.goods.save.table")

    val sqlSale = "select c.id, g.channel_id, g.goods_code, g.sale_cum_num, g.sale_cum_amount, g.index_type" +
                  " from  idmdata.m_sr_eg_goods_num_amount g " +
                  " inner join idmdata.dim_search_category c on c.goods_sid = g.goods_code and g.channel_id = c.channel_sid " +
                  " where g.cdate = " + date

    val rawData = readHive(sc, sqlSale)
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
    val pvUVRawData = readHive(sc, sqlVisit)
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
    val evaRawData = readHive(sc, sqlEva)
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

    val evaScoreRawData = readHive(sc, sqlEvaScore)
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
    val evaRation = readHive(sc, sqlEvaRatio)
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
    /**
    import hiveContext.implicits._
    val r = hasChannel.union(noChannel).reduceByKey(_ + _).coalesce(1).map{ case ((goodsID, category, channel), weight) =>
            Score(date, category, channel, goodsID, weight)}.toDF()

    r.registerTempTable("tmp")

      */

    val allGoods = hasChannel.union(noChannel)
    allGoods.cache()

    //every category take top 30
    val top30 = allGoods.map { case ((goodsID, category, channel), weight) => ((category, channel), Seq((goodsID, weight))) }
      .reduceByKey(_ ++ _).mapValues(_.sortWith(_._2 > _._2).take(30)).map { item => item._2.map(t => ((t._1, item._1._2), (item._1._1, t._2))) }.flatMap (s => s)

    val backCategory = hiveContext.sql("select sid, category_id, channel_sid from recommendation.goods_avaialbe_for_sale_channel where channel_sid = 3")

    val a = backCategory.rdd.distinct().map { row => if (row.anyNull) null else ((row.getString(0), row.getString(2)), row.getLong(1).toString) }.filter(_ != null)
    val b = top30.join(a).map { case ((goodsID, channel), ((searchCategory, score), backCategory)) => ((backCategory, channel), Seq((goodsID, score))) }
      .reduceByKey(_ ++ _).mapValues(_.sortWith(_._2 > _._2)).map { case ((backCategory, channel), goods) => ( "rcmd_goods_comp_ranking_" + backCategory, goods.map(_._1).mkString("#"))}
    val accumulator = sc.accumulator(0)
    val jedisCount = sc.accumulator(0)

    RedisClient.sparkKVToRedis(b, jedisCount, "cluster")

    // category top 30
//    val categoryTop = allGoods.map { case ((goodsID, category, channel), weight) => ((category, channel), Seq((weight))) }
//      .reduceByKey(_ ++ _).mapValues { s => s.sum / s.length }

    val categoryTop = allGoods.map { case ((goodsID, category, channel), weight) => ((goodsID, channel), weight) }.join(a)
      .map { case ((goods, channel), (weight, backCategory)) => ((backCategory, channel), Seq((weight)))}
      .reduceByKey(_ ++ _).mapValues { s => s.sum }




    categoryTop.cache()
    val allSort = categoryTop.collect().sortWith(_._2 > _._2).map(_._1).map(s => s._1).mkString("#")
    val pcSort = categoryTop.filter(_._1._2 == "3").collect().sortWith(_._2 > _._2).map(_._1).map(s => s._1).mkString("#")
    val h5Sort = categoryTop.filter(_._1._2 == "2").collect().sortWith(_._2 > _._2).map(_._1).map(s => s._1).mkString("#")
    val appSort = categoryTop.filter(_._1._2 == "1").collect().sortWith(_._2 > _._2).map(_._1).map(s => s._1).mkString("#")

    val jedis = RedisClient.jedisCluster
    jedis.set("rcmd_category_comp_ranking", allSort)
    if (pcSort != null) jedis.set("rcmd_category_comp_ranking_3", pcSort)
    if (h5Sort != null) jedis.set("rcmd_category_comp_ranking_2", h5Sort)
    if (appSort != null) jedis.set("rcmd_category_comp_ranking_1", appSort)

//    hiveContext.sql(s"insert overwrite table $hiveTable  partition(dt='$date') " +
//      s"select cdate, category, channel_id, goods_code, score from tmp")

  }

//  def calculator(): Double = {
//
//  }

  def  f(seq : Traversable[(String, Int)], weight: Double): Traversable[(String, Double)]  =
  {
    val max = seq.map(_._2).max
    val min = seq.map(_._2).min
    seq.map(item => (item._1, weight * (item._2 - min) / (max - min)))
  }

}
case class Score(cdate: String, category: String, channel_id: String, goods_code: String, score: Double)

object SearchCommandLineParser {
  val options = new Options

  val help = new Option("h", "help", false, "print help information")
  val prefix = new Option("p", "prefix", true, "sorted category key prefix in redis")

  options.addOption(help)
  options.addOption(prefix)

  val basicParser = new BasicParser

  def parser(args: Array[String]): Map[String, String] = {
    val commandLine = basicParser.parse(options, args)

    if (commandLine.hasOption("h")){
      printHelper()
      sys.exit(-1)
    }

    val optionMap = SearchConf.optionMap

    if (commandLine.hasOption(SearchConf.prefix)) {
      optionMap(SearchConf.prefix) = commandLine.getOptionValue(SearchConf.prefix)
    }
    if (commandLine.hasOption(SearchConf.day)) {
      optionMap(SearchConf.day) = "true"
    }

    optionMap.toMap

  }

  def printHelper(): Unit = {
    val helpFormatter = new HelpFormatter
    helpFormatter.printHelp("Category", options)
  }
}

object SearchConf {
  val day = "search.date"
  val prefix = "date.prefix"
  val optionMap = mutable.Map(prefix -> "rcmd_goods_comp_ranking_")

}