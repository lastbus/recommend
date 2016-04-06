package com.bl.bigdata.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.Test

import scala.io.Source

/**
  * Created by MK33 on 2016/4/5.
  */
@Test
class TupleSimilarityTest {

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("tuple similarity test")
  val sc = new SparkContext(sparkConf)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  @Test
  def calculate = {
    val data = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("data.txt")).
      getLines().toArray.map(s => {val w = s.split(","); (w(0), w(1))})
    val rdd = sc.parallelize(data)
    Console println "i = 1"
    TupleSimilarity calculate(rdd, 1) collect() foreach println
    Console println "i = 2"
    TupleSimilarity calculate(rdd, 2) collect() foreach println
    Console println "all"
    TupleSimilarity calculatorAB  rdd collect() foreach println
    val s = List(1,2,3,4,5)
    val s2 = s.toTraversable
    s2
  }
}
