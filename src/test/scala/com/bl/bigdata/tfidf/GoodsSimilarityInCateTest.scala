package com.bl.bigdata.tfidf

import org.apache.spark.mllib.linalg.{SparseVector, _}
import org.junit.Test

/**
  * Created by MK33 on 2016/10/27.
  */

@Test
class GoodsSimilarityInCateTest {

  @Test
  def addToItemTest = {

    val a1 = Array(("", 0.0), ("", 0.0), ("", 0.0))
    val a1_ = GoodsSimilarityInCate.addToItem(a1, ("a", 0.1))
    val a1__ = Array(("a", 0.1), ("", 0.0), ("", 0.0))
    for (i <- 0 until a1.length) {
      assert(a1_(i) == a1__(i))
    }

    val a11 = GoodsSimilarityInCate.addToItem(a1_, ("b", 2))
    val a11_ =  Array(("b", 2), ("a", 0.1), ("", 0.0))
    for (i <- 0 until a1.length) {
      assert(a11_(i) == a11(i))
    }

    val a111 = GoodsSimilarityInCate.addToItem(a11, ("c", 0.05))
    val a111_ =  Array(("b", 2), ("a", 0.1), ("c", 0.05))
    for (i <- 0 until a1.length) {
      assert(a111_(i) == a111(i))
    }


  }

  @Test
  def mergeArrayTest = {

    val a1 = Array(("a", 4.0), ("b", 3.0), ("c", 2.0), ("d", 1.0))
    val a2 = Array(("I", 9.0), ("II", 5.0), ("III", 2.0), ("IV", 1.0))

    val a = GoodsSimilarityInCate.mergeArray(a1, a2)
    val a_ = Array(("I", 9.0), ("II", 5.0), ("a", 4.0), ("b", 3.0), ("c", 2.0))
    println(a.mkString(","))
    for (i <- 0 until a1.length) {
      assert(a_(i) == a(i))
    }

  }


  @Test
  def vectorMultiply = {

    val v1 = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)).asInstanceOf[SparseVector]
    val v2 = Vectors.sparse(3, Array(0, 1), Array(2.0, 3.0)).asInstanceOf[SparseVector]
    var sum = 0.0
    for (v <- v1.indices) {
      sum +=v1(v) * v2(v)
    }
    val s1 = v1.values.map(s => s * s).sum
    val s2 = v2.values.map(s => s * s).sum

    println(sum / Math.sqrt(s1 * s2))

  }


}
