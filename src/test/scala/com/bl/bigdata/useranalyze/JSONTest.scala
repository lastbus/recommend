package com.bl.bigdata.useranalyze

import java.util

import org.json.JSONObject
import org.junit.Test


/**
 * Created by MK33 on 2016/5/16.
 */

@Test
class JSONTest {

  @Test
  def testJsonArray = {
    val jsonArray = new JSONObject()

    val json1 = new JSONObject()
    val map1 = new util.HashMap[String, String]()
    json1.put("k1", "1")
    json1.put("k2", "2")
    json1.put("k3", "3")
//    json1.put("cate1", map1)
    jsonArray.put("category", json1)

    val json2 = new JSONObject()
    val map2 = new util.HashMap[String, String]()
    json2.put("k11", "1")
    json2.put("k22", "2")
    json2.put("k33", "3")
//    json2.put("cate2", map2)
    jsonArray.put("category2", json2)

    println(jsonArray.toString())

  }

}
