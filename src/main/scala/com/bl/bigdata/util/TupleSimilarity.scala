package com.bl.bigdata.util

import org.apache.spark.rdd.RDD

/**
  * A B 的相似度：
  * FAB : AB 一起出现的频率，
  * FA : A 出现的频率
  * FB : B 出现的频率
  * Created by MK33 on 2016/4/5.
  */
object TupleSimilarity {

  /**
    * 计算 item 的相似度
    * @param tuple
    * @param strategy
    * @return
    */
  def calculateSimilarity(tuple: RDD[(String, String)], strategy: Int): RDD[(String, String, Double)] = {
    strategy match {
      case 0 => calculatorAB(tuple)
      case 1 => calculate(tuple, 1)
      case 2 => calculate(tuple, 2)
      case other: Int => throw new Exception(s"illegal number: $other")
    }
  }

  /**
    * FAB/FA
    * @param tuple (A, B)
    */
  def calculate(tuple: RDD[(String, String)], i: Int): RDD[(String, String, Double)] = {
    val countAB = tuple.map((_, 1)).reduceByKey(_ + _).map(t => (t._1._1, (t._1._2, t._2)))
    val count = tuple.map( t =>Array((t._1, 1), (t._2, 1))).flatMap(s => s).reduceByKey(_ + _)
    if ( i == 1 ) {
      countAB.join(count).map{ case (a,((b, freqAB), freqA)) => (a, b, freqAB.toDouble / freqA)}
    } else if ( i == 2 ) {
      countAB.map(s => (s._2._1, (s._1, s._2._2))).join(count).map{ case (b,((a, freqAB), freqA)) => (a, b, freqAB.toDouble / freqA)}
    } else {
      throw new Exception(s"error parameter $i.")
    }
  }

  /**
    * FAB / (FA * FB)`1/2`
    * @param tuple (A, B)
    */
  def calculatorAB(tuple: RDD[(String, String)]): RDD[(String, String, Double)] = {
    val countAB = tuple.map((_, 1)).reduceByKey(_ + _).map(t => (t._1._1, (t._1._2, t._2)))
    val count = tuple.map( t =>Array((t._1, 1), (t._2, 1))).flatMap(s => s).reduceByKey(_ + _)
    countAB.join(count)
      .map{ case (a,((b, freqAB), freqA)) => (b, (a, freqAB, freqA))}.join(count)
      .map{ case (b, ((a, freqAB, freqA), freqB)) => (a, b, freqAB.toDouble / Math.pow(freqA * freqB, 0.5))}
  }

}
