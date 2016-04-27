package com.bl.bigdata.util

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.xml.XML

/**
  * Created by MK33 on 2016/4/11.
  */
object SparkFactory {
  private[this] var sc: SparkContext = _
  private[this] var hiveContext: HiveContext = _

  /** */
  def getSparkContext(appName: String): SparkContext = {
    if (sc == null) {
      val sparkConf = new SparkConf().setAppName(appName)
      val confFile = ConfigurationBL.get("extra.configuration.file", "")
      // 加载hadoop其他配置文件，比如 hbase-site.xml、hive-site.xml等等符合hadoop配置文件规则的xml文件
      if ( !confFile.isEmpty )
        for ( conf <- confFile.split(":") ) {
          val xml = XML.load(conf)
          val properties = xml \ "property"
          for (property <- properties)
            sparkConf.set((property \ "name").text, (property \ "value").text)
        }
      sc = new SparkContext(sparkConf)
      sc
    } else {
      // 如果sc已经初始化了，那么传递给spark集群的参数则对集群没有影响了
      sc
    }
  }

    def getSparkContext: SparkContext = {
      if (sc == null) getSparkContext("recommend")
      else sc
    }


  def destroyResource(): Unit ={
    if (sc != null) sc.stop()
  }


  def getHiveContext : HiveContext = {
    if (hiveContext != null) hiveContext
    else if (hiveContext == null && sc != null) {hiveContext = new HiveContext(sc); hiveContext}
    else {
      sc = getSparkContext
      hiveContext = new HiveContext(sc)
      hiveContext
    }
  }



}
