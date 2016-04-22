package com.bl.bigdata

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

/**
 * Created by MK33 on 2016/4/21.
 */
object TestSpark {

  def main(args: Array[String]) {
    for (key <- System.getProperties.stringPropertyNames()) {
      println(key + "\t" + System.getProperty(key))
    }
    val sparkConf = new SparkConf().setAppName("test")

    val sc = new SparkContext(sparkConf)

  }

}
