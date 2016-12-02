package com.bl.bigdata.mail

import org.junit.Test

/**
  * Created by MK33 on 2016/3/29.
  */
@Test
class MailTest {

  @Test
  def test = {
//    MailServer.send("test method: send(msg: String)")
    val msg = Array("array0", "array1", "array2")
//    MailServer.send(msg)
  }

  @Test
  def group = {
    val array = Array(("a", 1), ("a", 2), ("b", 33), ("b", 9), ("a", 90))
    array.groupBy(_._1).map(s => (s._1, s._2.map(_._2).sum)).foreach(println)

  }
}
