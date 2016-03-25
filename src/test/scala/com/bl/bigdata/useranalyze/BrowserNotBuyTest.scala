package com.bl.bigdata.useranalyze

import com.bl.bigdata.useranalyze.BrowserNotBuy
import org.junit._

/**
  * Created by MK33 on 2016/3/24.
  */
@Test
class BrowserNotBuyTest {

  @Test
  def testFormat = {
    val categoryGoodsID = Array(("c1", "g1"), ("c2", "g2"), ("c3", "g3"),
                                ("c1", "g12"), ("c2", "g22"), ("c3", "g32"),
                                ("c1", "g13"), ("c2", "g23"), ("c3", "g33"))
    val result = """c1:g1,g12,g13#c2:g2,g22,g23#c3:g3,g32,g33"""

    val r = (new BrowserNotBuy).format(categoryGoodsID)
    println(r)
    assert(r.contains("c1:g1,g12,g13") &&
      r.contains("c2:g2,g22,g23") &&
      r.contains("c3:g3,g32,g33") &&
      r.split("#").size == 3)

  }
}
