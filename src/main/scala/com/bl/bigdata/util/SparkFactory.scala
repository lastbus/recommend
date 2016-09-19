package com.bl.bigdata.util

import com.bl.bigdata.mail.Message
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

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
