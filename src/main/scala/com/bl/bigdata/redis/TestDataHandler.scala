package com.bl.bigdata.redis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by MK33 on 2016/6/23.
 */
object TestDataHandler {

  def main(args: Array[String]) {
//    val in = Source.fromFile("D:/test-1.csv")
//    in.getLines().foreach(println(_))

    Logger.getRootLogger.setLevel(Level.OFF)
    val sc = new SparkContext(new SparkConf().setAppName("tests"))
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    var data = sc.textFile("D:\\test-1.csv").map(s => {
      val d = s.split(",")
      R(1, d(0).substring(1, d(0).length - 1), d(1).substring(1, d(1).length -1))
    }).toDF

    for (i <- 2 to 20) {
      val tmp = sc.textFile(s"D:\\test-$i.csv").map(s => {
        val d = s.split(",")
        R(i, d(0).substring(1, d(0).length - 1), d(1).substring(1, d(1).length -1))
      }).toDF
      data = data.unionAll(tmp)
    }

    data.cache()
    println("===")

    println("data count: " + data.count())

//    data.foreach(println)
    println(data.map(_(0)).distinct().count())
    println(data.map(_(1)).distinct().count())
    println(data.map(_(2)).distinct().count())

//    data.map(_(1)).distinct().foreach(println)
    println(data.map(r => r.getString(2).toDouble).max)
    println(data.map(r => r.getString(2).toDouble).min)

    data.printSchema()

    val vector = data.map(r => (r.getString(1), Seq((r.getInt(0), r.getString(2))))).reduceByKey(_ ++ _)
      .mapValues(_.sortWith(_._1 < _._1)).mapValues(_.map(_._2).mkString(" ")).map(s => s._1 + "," + s._2)

    vector.foreach(println)




  }


  def readCSV(sc: SparkContext, path: String, round: Int) = {

  }

}

case class R(count: Int, key: String, value: String)