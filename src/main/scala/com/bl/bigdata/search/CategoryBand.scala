package com.bl.bigdata.search

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

import com.bl.bigdata.datasource.ReadData._
import com.bl.bigdata.util.SparkFactory
import org.apache.commons.cli.{BasicParser, HelpFormatter, Option, Options}

import scala.collection.mutable

/**
  * Created by MK33 on 2016/7/11.
  */
object CategoryBand {

  def main(args: Array[String]) {

    val options = CategoryScoreConf.parser(args)

    val hiveTableName = options(CategoryScoreConf.to)
    val dateFormat = options(CategoryScoreConf.dateFormat)
    val decimalFormatter = options(CategoryScoreConf.decimalFormat)


    val sdf = new SimpleDateFormat(dateFormat)
    val cal = Calendar.getInstance()
    cal.setTime(new Date)
    cal.add(Calendar.DAY_OF_MONTH, -1)
    val date = if (options.contains(CategoryScoreConf.date)) options(CategoryScoreConf.date)  else sdf.format(cal.getTime)

    val sqlSpu = " SELECT  c.lev1_sid, c.lev2_sid, c.lev3_sid, c.lev4_sid, c.lev5_sid, " +
                 "         g.brand_sid, g.pro_sid  " +
                " FROM idmdata.dim_search_category c  " +
                s" INNER JOIN sourcedata.s06_pcm_mdm_goods g ON c.goods_sid = g.sid  AND g.dt = '$date' " +
                s" where isnotnull(g.brand_sid) and g.brand_sid <> 'null' "

    val sc = SparkFactory.getSparkContext("search.category")
    val spuRDD = readHive(sc, sqlSpu).map { case Array(lev1, lev2, lev3, lev4, lev5, brand, productId) =>
        ((lev1, lev2, lev3, lev4, lev5, brand), productId)
      }.distinct().mapValues(_ => 1).reduceByKey(_ + _).map { s => ((s._1._1,s._1._2,s._1._3, s._1._4, s._1._5), Seq((s._1._6, s._2)))}.reduceByKey(_ ++ _)
      .map{ case (a, s) =>
        val min = s.minBy(_._2)._2
        val max = s.maxBy(_._2)._2
        val divide = if (min == max) 1 else max - min
        s.map(t => ((a._1, a._2, a._3, a._4, a._5, t._1), 30.0 * (t._2 - min) / divide))
      }.flatMap(s => s)

    spuRDD.cache()

    val sqlSell = " SELECT  c.lev1_sid, c.lev2_sid, c.lev3_sid, c.lev4_sid, c.lev5_sid, " +
      "         g.brand_sid, g.pro_sid, n.sale_cum_amount, n.index_type  " +
      " FROM idmdata.m_sr_eg_goods_num_amount n " +
      " INNER JOIN idmdata.dim_search_category c ON c.goods_sid = n.goods_code  " +
      s" INNER JOIN sourcedata.s06_pcm_mdm_goods g ON g.sid = n.goods_code AND g.dt = '$date' " +
      " where n.cdate = " + date + " and (isnotnull(g.brand_sid) and g.brand_sid <> 'null') "


    val sellRDD = readHive(sc, sqlSell).map { case Array(lev1, lev2, lev3, lev4, lev5, brand, productId, saleMoney, indexType) =>
      ((lev1, lev2, lev3, lev4, lev5, brand, indexType), saleMoney.toDouble)
    }.reduceByKey(_ + _).map(s => ((s._1._1, s._1._2, s._1._3, s._1._4, s._1._5, s._1._7), Seq((s._1._6, s._2 )))).reduceByKey(_ ++ _)
      .map { case (l, brands) =>
          val min = brands.minBy(_._2)._2
          val max = brands.maxBy(_._2)._2
          val divider = if (max - min == 0.0) 1 else max - min
          brands.map(s => ((l._1, l._2, l._3, l._4, l._5, s._1),
            if (l._6 == "1")  40.0 * (s._2 - min) / divider else if ((l._6 == "2")) 30.0 * (s._2 - min) / divider else 0.0))
      }.flatMap(s => s)
    sellRDD.cache()

    val result = spuRDD.union(sellRDD).reduceByKey(_ + _)
    result.cache()
    val hiveSql = SparkFactory.getHiveContext
    import hiveSql.implicits._

    val leve5 = result.filter(_._1._5 != null).map(s => ((s._1._5, s._1._6), s._2)).reduceByKey(_ + _)
      .map(s =>{val decimalFormat = new DecimalFormat(decimalFormatter); CategoryScore(s._1._1, s._1._2, decimalFormat.format(s._2).toDouble) }).toDF().registerTempTable("L5")
    val leve4 = result.filter(_._1._4 != null).map(s => ((s._1._4, s._1._6), s._2)).reduceByKey(_ + _)
      .map(s => {val decimalFormat = new DecimalFormat(decimalFormatter); CategoryScore(s._1._1, s._1._2, decimalFormat.format(s._2).toDouble) }).toDF().registerTempTable("L4")
    val leve3 = result.filter(_._1._3 != null).map(s => ((s._1._3, s._1._6), s._2)).reduceByKey(_ + _)
      .map(s => {val decimalFormat = new DecimalFormat(decimalFormatter); CategoryScore(s._1._1, s._1._2, decimalFormat.format(s._2).toDouble) }).toDF().registerTempTable("L3")
    val leve2 = result.filter(_._1._4 != null).map(s => ((s._1._2, s._1._6), s._2)).reduceByKey(_ + _)
      .map(s => {val decimalFormat = new DecimalFormat(decimalFormatter); CategoryScore(s._1._1, s._1._2, decimalFormat.format(s._2).toDouble) }).toDF().registerTempTable("L2")
    val leve1 = result.filter(_._1._4 != null).map(s => ((s._1._1, s._1._6), s._2)).reduceByKey(_ + _)
      .map(s => {val decimalFormat = new DecimalFormat(decimalFormatter); CategoryScore(s._1._1, s._1._2, decimalFormat.format(s._2).toDouble) }).toDF().registerTempTable("L1")

    hiveSql.sql(s"insert overwrite table $hiveTableName partition(dt='$date')  " +
      " select level, brand, score from L1 union all select level, brand, score from L2  " +
      "union all select level, brand, score from L3 union all select level, brand, score from L4  " +
      "union all select level, brand, score from L5 ")


  }

}

case class CategoryScore(level: String, brand: String, score: Double)

object CategoryScoreConf {

  val options = new Options()

  val basicParser = new BasicParser

  val dataSource = "data.source"
  val to = "hive.table.name"
  val dateFormat = "data.format"
  val date = "date"
  val decimalFormat = "decimal.format"
  val spuWeight = "spu.weight"
  val saleMoney7 = "sale.money.weight.7"
  val saleMoney30 = "sale.money.weight.30"

  val optionsMap = mutable.Map(dateFormat -> "yyyyMMdd", decimalFormat -> "#.000",
                               spuWeight -> "30.0", saleMoney7 -> "40.0",
                               saleMoney30 -> "30.0", to -> "test.search_category_score",
                               dataSource -> "hive")

  addOption("h", "help", false, "print help information")

  addOption("ds", dataSource, true, "data source: default: hive")
  addOption("hive", to, true, " save result to where ")

  addOption("df", dateFormat, true, "date format, default: yyyyMMdd")
  addOption("d", date, true, "date to execute, default: no definition")
  addOption("dec", decimalFormat, true, "decimal format: default: #.000")
  addOption("wspu", spuWeight, true, "spu weight, default: 30.0 ")
  addOption("s7", saleMoney7, true, "7 days sale money weight, default: 40.0")
  addOption("s30", saleMoney30, true, "30 days sale money weight, default: 30.0")

  def parser(args: Array[String]): Map[String, String] = {
    val commandLineParser = basicParser.parse(options, args)

    val map = optionsMap.clone()

    if (commandLineParser.hasOption("h")){
      printHelp()
      sys.exit(-1)
    }
    if (commandLineParser.hasOption(to)) map(to) = commandLineParser.getOptionValue(to)
    if (commandLineParser.hasOption(dateFormat)) map(dateFormat) = commandLineParser.getOptionValue(dateFormat)
    if (commandLineParser.hasOption(date)) map(date) = commandLineParser.getOptionValue(date)
    if (commandLineParser.hasOption(decimalFormat)) map(decimalFormat) = commandLineParser.getOptionValue(decimalFormat)
    if (commandLineParser.hasOption(spuWeight)) map(spuWeight) = commandLineParser.getOptionValue(spuWeight)
    if (commandLineParser.hasOption(saleMoney7)) map(saleMoney7) = commandLineParser.getOptionValue(saleMoney7)
    if (commandLineParser.hasOption(saleMoney30)) map(saleMoney30) = commandLineParser.getOptionValue(saleMoney30)

    map.toMap
  }

  def addOption(shortName: String, longName: String, hasArgs: Boolean, description: String): Unit = {
    options.addOption(shortName, longName, hasArgs, description)
  }

  def addOption(shortName: String, longName: String, hasArgs: Boolean, description: String, mustHave: Boolean): Unit = {
    if (mustHave){
      val op = new Option(shortName, longName, hasArgs, description)
      op.setRequired(true)
    } else {
      addOption(shortName, longName, hasArgs, description)
    }

  }

  def printHelp(): Unit = {
    val helperFormat = new HelpFormatter
    helperFormat.printHelp("search.category", options)
  }
}
