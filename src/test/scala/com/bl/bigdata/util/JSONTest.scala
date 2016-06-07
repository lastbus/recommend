package com.bl.bigdata.util

import org.json.JSONObject
import org.junit.Test

/**
 * Created by MK33 on 2016/6/7.
 */
@Test
class JSONTest {

  @Test
  def test = {
    val json = new JSONObject()
    json.put("gg", 0.22947554746688675)
    println(json.getDouble("gg"))

    val t = Thread.currentThread().getClass.getClassLoader.getResource("")
  }
}
