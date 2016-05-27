package com.bl.bigdata.util

import org.json.JSONObject
import org.junit.Test

/**
 * Created by MK33 on 2016/5/25.
 */
@Test
class Json {

  @Test
  def tet = {
    val testJson = "{\"actType\":\"view\",\"cookieId\":\"haojutao78make\",\"eventDate\":\"1464179985785\",\"goodsId\":\"80703\"}"

    val json = new JSONObject(testJson)
    println(json.get("actType"))

  }
}
