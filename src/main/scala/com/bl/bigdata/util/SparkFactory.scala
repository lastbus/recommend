package com.bl.bigdata.util

import com.bl.bigdata.mail.Message
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
  def getSparkContext(appName: String = "recommend"): SparkContext = {
    if (sc == null) {
      val sparkConf = new SparkConf().setAppName(appName)
      require(ConfigurationBL.getAll.length > 0)
      val confFile = ConfigurationBL.get("extra.configuration.file", "")
      // 加载hadoop其他配置文件，比如 hbase-site.xml、hive-site.xml等等符合hadoop配置文件规则的xml文件
      if ( !confFile.isEmpty )
        for ( conf <- confFile.split(",") ) {
          val xml = XML.load(conf)
          val properties = xml \ "property"
          for (property <- properties)
            sparkConf.set((property \ "name").text, (property \ "value").text)
        }
      sc = new SparkContext(sparkConf)
      Message.addMessage("application-id:\t" + sc.applicationId)
      Message.addMessage("application-master:\t" + sc.master)

      sc
    } else sc

  }

    def getSparkContext: SparkContext = {
      if (sc == null) getSparkContext()
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
