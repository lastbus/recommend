package com.bl.bigdata.search

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

import com.bl.bigdata.util.{DataBaseUtil, MyCommandLine, SparkFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Created by MK33 on 2016/7/11.
  */
object CategoryBand {

  def main(args: Array[String]) {

    val options = CategoryScoreConf.parser(args)

    val input = options(CategoryScoreConf.input)
    val sqlSpu = options(CategoryScoreConf.sqlSpu)
    val sqlSell = options(CategoryScoreConf.sqlSell)

    val hiveTableName = options(CategoryScoreConf.to)
    val hiveTableName2 = options(CategoryScoreConf.to2)

    val dateFormat = options(CategoryScoreConf.dateFormat)
    val decimalFormatter = options(CategoryScoreConf.decimalFormat)

    val sdf = new SimpleDateFormat(dateFormat)
    val cal = Calendar.getInstance()
    cal.setTime(new Date)
    cal.add(Calendar.DAY_OF_MONTH, -1)
    val date = if (options.contains(CategoryScoreConf.date)) options(CategoryScoreConf.date)  else sdf.format(cal.getTime)

    val sqlSpu0 = " SELECT  c.lev1_sid, c.lev2_sid, c.lev3_sid, c.lev4_sid, c.lev5_sid, " +
                 "         g.brand_sid, g.pro_sid  " +
                " FROM idmdata.dim_search_category c  " +
                s" INNER JOIN sourcedata.s06_pcm_mdm_goods g ON c.goods_sid = g.sid  AND g.dt = '$date' " +
                s" where isnotnull(g.brand_sid) and g.brand_sid <> 'null' "

    val spuRDD = DataBaseUtil.getData(input, sqlSpu, date, date, date).map { case Array(lev1, lev2, lev3, lev4, lev5, brand, productId) =>
        ((lev1, lev2, lev3, lev4, lev5, brand), productId)
      }.distinct().mapValues(_ => 1).reduceByKey(_ + _).map { s => ((s._1._1,s._1._2,s._1._3, s._1._4, s._1._5), Seq((s._1._6, s._2)))}.reduceByKey(_ ++ _)
      .map{ case (a, s) =>
        val min = s.minBy(_._2)._2
        val max = s.maxBy(_._2)._2
        val divide = if (min == max) 1 else max - min
        s.map(t => ((a._1, a._2, a._3, a._4, a._5, t._1), 30.0 * (t._2 - min) / divide))
      }.flatMap(s => s)

    val sqlSell0 = " SELECT  c.lev1_sid, c.lev2_sid, c.lev3_sid, c.lev4_sid, c.lev5_sid, " +
      "         g.brand_sid, g.pro_sid, n.sale_cum_amount, n.index_type  " +
      " FROM idmdata.m_sr_eg_goods_num_amount n " +
      " INNER JOIN idmdata.dim_search_category c ON c.goods_sid = n.goods_code  " +
      s" INNER JOIN sourcedata.s06_pcm_mdm_goods g ON g.sid = n.goods_code AND g.dt = '$date' " +
      " where n.cdate = " + date + " and (isnotnull(g.brand_sid) and g.brand_sid <> 'null') "

    val sellRDD = DataBaseUtil.getData(input, sqlSell, date, date, date, date).map { case Array(lev1, lev2, lev3, lev4, lev5, brand, productId, saleMoney, indexType) =>
      ((lev1, lev2, lev3, lev4, lev5, brand, indexType), saleMoney.toDouble)
    }.reduceByKey(_ + _).map(s => ((s._1._1, s._1._2, s._1._3, s._1._4, s._1._5, s._1._7), Seq((s._1._6, s._2 )))).reduceByKey(_ ++ _)
      .map { case (l, brands) =>
          val min = brands.minBy(_._2)._2
          val max = brands.maxBy(_._2)._2
          val divider = if (max - min == 0.0) 1 else max - min
          brands.map(s => ((l._1, l._2, l._3, l._4, l._5, s._1),
            if (l._6 == "1")  40.0 * (s._2 - min) / divider else if ((l._6 == "2")) 30.0 * (s._2 - min) / divider else 0.0))
      }.flatMap(s => s)

    val result = spuRDD.union(sellRDD).reduceByKey(_ + _)
    result.persist(StorageLevel.MEMORY_AND_DISK_2)
    val hiveSql = SparkFactory.getHiveContext
    import hiveSql.implicits._

    // (category, brand, score)
    result.filter(_._1._5 != null).map(s => ((s._1._5, s._1._6), s._2)).reduceByKey(_ + _).map(s => (s._1._1, s._1._2, s._2))
      .map(s => (s._1, Seq((s._2, s._3)))).reduceByKey(_ ++ _).map { case (lev, goods) =>
      val max = goods.map(_._2).max
      val min = goods.map(_._2).min
      val divider = if (max - min == 0.0) 1 else max - min
      goods.map(g => (lev, g._1, 100.0 * (g._2 - min) / divider ))
    }.flatMap(s => s)
      .map(s =>{val decimalFormat = new DecimalFormat(decimalFormatter); CategoryScore(s._1, s._2, decimalFormat.format(s._3).toDouble) }).toDF().registerTempTable("L5")


    result.filter(_._1._4 != null).map(s => ((s._1._4, s._1._6), s._2)).reduceByKey(_ + _).map(s => (s._1._1, s._1._2, s._2))
      .map(s => (s._1, Seq((s._2, s._3)))).reduceByKey(_ ++ _).map { case (lev, goods) =>
      val max = goods.map(_._2).max
      val min = goods.map(_._2).min
      val divider = if (max - min == 0.0) 1 else max - min
      goods.map(g => (lev, g._1, 100.0 * (g._2 - min) / divider ))
    }.flatMap(s => s)
      .map(s => {val decimalFormat = new DecimalFormat(decimalFormatter); CategoryScore(s._1, s._2, decimalFormat.format(s._3).toDouble) }).toDF().registerTempTable("L4")


    result.filter(_._1._3 != null).map(s => ((s._1._3, s._1._6), s._2)).reduceByKey(_ + _).map(s => (s._1._1, s._1._2, s._2))
      .map(s => (s._1, Seq((s._2, s._3)))).reduceByKey(_ ++ _).map { case (lev, goods) =>
      val max = goods.map(_._2).max
      val min = goods.map(_._2).min
      val divider = if (max - min == 0.0) 1 else max - min
      goods.map(g => (lev, g._1, 100.0 * (g._2 - min) / divider ))
    }.flatMap(s => s)
      .map(s => {val decimalFormat = new DecimalFormat(decimalFormatter); CategoryScore(s._1, s._2, decimalFormat.format(s._3).toDouble) }).toDF().registerTempTable("L3")


    result.filter(_._1._2 != null).map(s => ((s._1._2, s._1._6), s._2)).reduceByKey(_ + _).map(s => (s._1._1, s._1._2, s._2))
      .map(s => (s._1, Seq((s._2, s._3)))).reduceByKey(_ ++ _).map { case (lev, goods) =>
      val max = goods.map(_._2).max
      val min = goods.map(_._2).min
      val divider = if (max - min == 0.0) 1 else max - min
      goods.map(g => (lev, g._1, 100.0 * (g._2 - min) / divider ))
    }.flatMap(s => s)
      .map(s => {val decimalFormat = new DecimalFormat(decimalFormatter); CategoryScore(s._1, s._2, decimalFormat.format(s._3).toDouble) }).toDF().registerTempTable("L2")


    result.filter(_._1._1 != null).map(s => ((s._1._1, s._1._6), s._2)).reduceByKey(_ + _).map(s => (s._1._1, s._1._2, s._2))
      .map(s => (s._1, Seq((s._2, s._3)))).reduceByKey(_ ++ _).map { case (lev, goods) =>
      val max = goods.map(_._2).max
      val min = goods.map(_._2).min
      val divider = if (max - min == 0.0) 1 else max - min
      goods.map(g => (lev, g._1, 100.0 * (g._2 - min) / divider ))
    }.flatMap(s => s)
      .map(s => {val decimalFormat = new DecimalFormat(decimalFormatter); CategoryScore(s._1, s._2, decimalFormat.format(s._3).toDouble) }).toDF().registerTempTable("L1")



    hiveSql.sql(s"insert overwrite table $hiveTableName partition(dt='$date')  " +
      " select level, brand, score from L1 union all select level, brand, score from L2  " +
      "union all select level, brand, score from L3 union all select level, brand, score from L4  " +
      "union all select level, brand, score from L5 ")

    // (category, score)
//    result.filter( _._1._1 != null).map(s => (s._1._1, s._2)).reduceByKey(_ + _)
//      .map(s =>{val decimalFormat = new DecimalFormat(decimalFormatter); CategoryScore2(s._1, decimalFormat.format(s._2).toDouble) }).toDF().registerTempTable("C1")

    result.filter( s => s._1._1 != null && s._1._2 != null).map(s => ((s._1._1, s._1._2), s._2)).reduceByKey(_ + _).map(s => (s._1._1, s._1._2, s._2))
    .map(s => (s._1, Seq((s._2, s._3)))).reduceByKey(_ ++ _).map { case (lev, goods) =>
      val max = goods.map(_._2).max
      val min = goods.map(_._2).min
      val divider = if (max - min == 0.0) 1 else max - min
      goods.map(g => (lev, g._1, 100.0 * (g._2 - min) / divider ))
    }.flatMap(s => s)
      .map(s =>{val decimalFormat = new DecimalFormat(decimalFormatter); CategoryScore2(s._2, s._1, decimalFormat.format(s._3).toDouble) }).toDF().registerTempTable("C2")

    result.filter(s => s._1._2 != null && s._1._3 != null).map(s => ((s._1._2, s._1._3), s._2)).reduceByKey(_ + _).map(s => (s._1._1, s._1._2, s._2))
    .map(s => (s._1, Seq((s._2, s._3)))).reduceByKey(_ ++ _).map { case (lev, goods) =>
      val max = goods.map(_._2).max
      val min = goods.map(_._2).min
      val divider = if (max - min == 0.0) 1 else max - min
      goods.map(g => (lev, g._1, 100.0 * (g._2 - min) / divider ))
    }.flatMap(s => s)
      .map(s =>{val decimalFormat = new DecimalFormat(decimalFormatter); CategoryScore2(s._2, s._1, decimalFormat.format(s._3).toDouble) }).toDF().registerTempTable("C3")

    result.filter( s => s._1._3 != null && s._1._4 != null).map(s => ((s._1._3, s._1._4), s._2)).reduceByKey(_ + _).map(s => (s._1._1, s._1._2, s._2))
    .map(s => (s._1, Seq((s._2, s._3)))).reduceByKey(_ ++ _).map { case (lev, goods) =>
      val max = goods.map(_._2).max
      val min = goods.map(_._2).min
      val divider = if (max - min == 0.0) 1 else max - min
      goods.map(g => (lev, g._1, 100.0 * (g._2 - min) / divider ))
    }.flatMap(s => s)
      .map(s =>{val decimalFormat = new DecimalFormat(decimalFormatter); CategoryScore2(s._2, s._1, decimalFormat.format(s._3).toDouble) }).toDF().registerTempTable("C4")

    result.filter( s => s._1._4 != null && s._1._5 != null).map(s => ((s._1._4, s._1._5), s._2)).reduceByKey(_ + _).map(s => (s._1._1, s._1._2, s._2))
    .map(s => (s._1, Seq((s._2, s._3)))).reduceByKey(_ ++ _).map { case (lev, goods) =>
      val max = goods.map(_._2).max
      val min = goods.map(_._2).min
      val divider = if (max - min == 0.0) 1 else max - min
      goods.map(g => (lev, g._1, 100.0 * (g._2 - min) / divider ))
    }.flatMap(s => s)
      .map(s =>{val decimalFormat = new DecimalFormat(decimalFormatter); CategoryScore2(s._2, s._1, decimalFormat.format(s._3).toDouble) }).toDF().registerTempTable("C5")

    hiveSql.sql(s"insert overwrite table $hiveTableName2 partition(dt='$date')  " +
      " select category, parentCate, score from C2 " +
      "union all select category, parentCate, score from C3  " +
      "union all select category, parentCate, score from C4  " +
      "union all select category, parentCate, score from C5 ")


  }

  /** 归一化处理 */
  def unify(rdd: RDD[(String, String, Double)]): RDD[(String, String, Double)] = {
    rdd.map(s => (s._1, Seq((s._2, s._3)))).reduceByKey(_ ++ _).map { case (lev, goods) =>
        val max = goods.map(_._2).max
        val min = goods.map(_._2).min
        val divider = if (max - min == 0.0) 1 else max - min
        goods.map(g => (lev, g._1, 100.0 * (g._2 - min) / divider ))
    }.flatMap(s => s)
  }



}

case class CategoryScore(level: String, brand: String, score: Double)
case class CategoryScore2(category: String, parentCate: String, score: Double)

object CategoryScoreConf {
  val input = "input"
  val sqlSpu = "sql.spu"
  val sqlSell = "sql.sell"
  val output = "output"
  val to = "hiveTableName1"
  val to2 = "hiveTableName2"
  val dateFormat = "data.format"
  val date = "date"
  val decimalFormat = "decimal.format"
  val spuWeight = "spu.weight"
  val saleMoney7 = "sale.money.weight.7"
  val saleMoney30 = "sale.money.weight.30"

  val myCommandLine = new MyCommandLine("SearchCategoryBand")

  myCommandLine.addOption("i", input, true, "input data source", "hive")
  myCommandLine.addOption("sqlSpu", sqlSpu, true, "sql name in hive.xml", "search.category.score.spu")
  myCommandLine.addOption("sqlSell", sqlSell, true, "sql name in hive.xml", "search.category.score.sell")
  myCommandLine.addOption("o", output, true, "output resut to where", "hive")
  myCommandLine.addOption("hive1", to, true, " search category brand score hive table", "test.search_category_score")
  myCommandLine.addOption("hive2", to2, true, "search category score hive table", "test.search_category_score2")
  myCommandLine.addOption("df", dateFormat, true, "date format", "yyyyMMdd")
  myCommandLine.addOption("d", date, true, "date to execute")
  myCommandLine.addOption("dec", decimalFormat, true, "decimal format", "#.000")
  myCommandLine.addOption("wspu", spuWeight, true, "spu weight", "30.0 ")
  myCommandLine.addOption("s7", saleMoney7, true, "7 days sale money weight", "40.0")
  myCommandLine.addOption("s30", saleMoney30, true, "30 days sale money weight", "30.0")

  def parser(args: Array[String]): Map[String, String] = {
    myCommandLine.parser(args)
  }

}
