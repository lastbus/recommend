package com.bl.bigdata.ranking

import org.junit._

/**
 * Created by MK33 on 2016/3/23.
 */
@Test
class HotSaleGoodsTest {
  val test = new HotSaleGoods

  @Test
  def testGetDateBeforeNow = {
//    assert(HotSaleGoods.getDateBeforeNow(0) == "2016-03-24")
//    assert(HotSaleGoods.getDateBeforeNow(7) == "2016-03-17")
    println("===============")
    println(HotSaleGoods.getDateBeforeNow(200))
    println("================")
  }

  @Test
  def testFilterDate = {
    assert(HotSaleGoods.filterDate(("", "", 0, "2016-03-03 09:39:11.0", "", ""), "2016-03-01") == true)
    assert(HotSaleGoods.filterDate(("", "", 0, "2016-03-01 00:00:00.0", "", ""), "2016-03-01") == true)
    assert(HotSaleGoods.filterDate(("", "", 0, "2016-03-01 09:39:11.0", "", ""), "2016-03-02") == false)
  }

}
