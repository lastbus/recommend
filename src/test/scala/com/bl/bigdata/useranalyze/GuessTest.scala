package com.bl.bigdata.useranalyze

import org.junit.Test

/**
 * Created by MK33 on 2016/4/18.
 */
@Test
class GuessTest {
  implicit def long2Int(a: Int) = a.toLong

  @Test
  def insertSort2Test = {

    val guess = new Guess

//    val array = Array(("1", 9.0), ("2", 4.0), ("4", 3.5), ("5", 2.6))
//    val v = ("4", 10.0)
//    val after = Array(("4", 10.0), ("1", 9.0), ("2", 4.0), ("4", 3.5))
//    assert(equal(guess.insertSort2(array, v), after))
//
//    val array2 = Array(("1", 9.0), ("2", 4.0), ("4", 3.5), ("5", 2.6))
//    val v2 = ("6", 9.0)
//    val after2 = Array(("1", 9.0), ("6", 9.0), ("2", 4.0), ("4", 3.5))
//    assert(equal(guess.insertSort2(array2, v2), after2))
//
//    val array3 = Array(("1", 9.0), ("2", 4.0), ("4", 3.5), ("5", 2.6))
//    val v3 = ("6", 5.0)
//    val after3 = Array(("1", 9.0), ("6", 5.0), ("2", 4.0), ("4", 3.5))
//    assert(equal(guess.insertSort2(array3, v3), after3))

//    val array4 = Array((1, 9.0), (2, 4.0), (4, 3.5), (5, 2.6)).map(s=>(s._1.toLong, s._2))
//    val v4 = (6L, 2.7)
//    val after4 = Array((1, 9.0), (2, 4.0), (4, 3.5), (6, 2.7))
//    assert(equal(guess.insertSort2(array4, v4), after4))
//
//    val array5 = Array((1, 9.0), (2, 4.0), (4, 3.5), (5, 2.6)).map(s=>(s._1.toLong, s._2))
//    val v5 = (6L, 2.7)
//    val after5 = Array((1, 9.0), (2, 4.0), (4, 3.5), (6, 2.7))
//    assert(equal(guess.insertSort2(array5, v5), after5))
//
//    val array6 = Array((1, 9.0), (2, 0.0), (4, 0.0), (5, 0.0)).map(s=>(s._1.toLong, s._2))
//    val v6 = (6L, 1.0)
//    val after6 = Array((1, 9.0), (6, 1.0), (2, 0.0), (4, 0.0))
//    assert(equal(guess.insertSort2(array6, v6), after6))
//
//    val array7 = Array((1, 0.0), (2, 0.0), (4, 0.0), (5, 0.0)).map(s=>(s._1.toLong, s._2))
//    val v7 = (6L, 1.0)
//    val after7 = Array((6, 1.0), (1, 0.0), (2, 0.0), (4, 0.0))
//    assert(equal(guess.insertSort2(array7, v7), after7))
  }

  def equal(arr1: Array[(String, Double)], arr2: Array[(String, Double)]): Boolean = {
    if(arr1.length != arr2.length) return false
    for (i <- 0 until arr1.length) {
      val a = arr1(i)
      val b = arr2(i)
      if ( a._1 != b._1 || Math.abs(a._2 - b._2) > Double.MinPositiveValue) return false
    }
    true
  }



}
