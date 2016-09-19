package com.bl.bigdata.util

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.xml.XML

/**
  * Created by MK33 on 2016/4/6.
  */
object HiveUtil {

  val logger = LogManager.getLogger(this.getClass.getName)

  def parse(hiveConf: String): Map[String, String] ={
    val inputStream = this.getClass.getClassLoader.getResourceAsStream(hiveConf)
    if (inputStream == null) {
      logger.error(s"cannot found $hiveConf ")
      return null
    }
    val xml = XML.load(this.getClass.getClassLoader.getResourceAsStream(hiveConf))
    val root = xml \ "sql"
    val sqls = mutable.Map[String, String]()
    for (sql <- root) {
      val name = (sql \ "name").text
      val value = (sql \ "value").text
      println(name + "  " + value)
      sqls(name) = value
    }
    sqls.toMap
  }


  def main(args: Array[String]) {
    val m = parse("hive.xml")
    m.foreach(println)

  }

}
