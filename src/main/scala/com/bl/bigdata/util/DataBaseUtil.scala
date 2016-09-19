package com.bl.bigdata.util

import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.xml.XML

/**
  * Created by MK33 on 2016/7/14.
  */
object DataBaseUtil {
  val logger = LogManager.getLogger(this.getClass.getName)

  lazy val hiveSqls = {
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("hive.xml")
    if (inputStream == null) {
      logger.error(s"cannot found hive.xml ")
      sys.exit(-1)
    }
    val xml = XML.load(inputStream)
    val root = xml \ "sql"
    val sqls = mutable.Map[String, String]()
    for (sql <- root) {
      val name = (sql \ "name").text
      val value = (sql \ "value").text
      logger.debug("\n" + name + "\n" + value)
      sqls(name) = value
    }
    sqls.toMap
  }


  def getData(name: String, args: String*): RDD[Array[String]] ={
    //TODO 这样写不是很通用
    name match {
      case "hive" =>
        if (args.length < 1) {logger.error("please input hive sql or and parameters"); sys.exit(-1)}
        loadHive(args)
      case _ =>
        logger.error(s"don't support database type:  $name")
        sys.exit(-1)
    }

  }

  /** load hive data, transform to spark RDD */
  def loadHive(sqls: Seq[String]): RDD[Array[String]] = {
    val tmp = hiveSqls(sqls(0))
    val sql = if (sqls.length > 1) String.format(tmp, sqls.tail:_*) else tmp
    val hiveContext = SparkFactory.getHiveContext
    hiveContext.sql(sql).map(row =>   row.toSeq.map( r => if (r == null) null else r.toString).toArray )
  }





}
