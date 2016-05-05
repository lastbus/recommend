package com.bl.bigdata.search

import org.apache.spark.rdd.RDD

/**
 * Created by MK33 on 2016/5/5.
 */
object CalculatorWeight
{

  def main(args: Array[String])
  {
    val seq = Array(("a", 1), ("b", 2), ("c", 5), ("v", 5), ("e", 10))
    val weight = 10
    val f2 = f(seq, weight)
    f2.foreach(println)

  }


  def  f[T](seq : Traversable[(T, Int)], weight: Double): Traversable[(T, Double)]  =
  {
    val max = seq.map(_._2).max
    val min = seq.map(_._2).min
    seq.map(item => (item._1, weight * (item._2 - min) / (max - min)))
  }

  /** 商品类别， 商品ID */
  def calculator[T](items: RDD[(T, String)],
                 f: Traversable[(T, Int)] => Traversable[(T, Double)]): RDD[(T, String, Double)] =
  {

//    val itemCount = items.map(s => (s._1, (s._2, 1)).reduceByKey((s1, s2) => (s1 + s2)).map( s=> (s._1._1, Seq((s._1._2, s._2)))).cache()
//    val a = itemCount.reduceByKey(_ ++ _)
//    val b= a.mapValues(s=> f(s)).map(s => for (i <- s._2) yield (s._1, i._1, i._2)).flatMap(s =>s)
//    b
    null
  }

}


