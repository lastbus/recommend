package com.bl.bigdata.useranalyze

import org.junit.Test

/**
 * Created by MK33 on 2016/4/18.
 */
@Test
class GuessTest {

  @Test
  def insertSortingTest = {
    val guess = new Guess
    val array = Array((1, 9.0), (2, 4.0), (4, 3.5), (5, 2.6))
    val v = (4, 10.0)
    val after = Array((4, 10.0), (1, 9.0), (2, 4.0), (4, 3.5))
    val result = guess.insertSort(array, v)
    array.equals(after)



  }
}
