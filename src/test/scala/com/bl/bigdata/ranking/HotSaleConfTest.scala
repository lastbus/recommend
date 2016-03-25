package com.bl.bigdata.ranking

import com.bl.bigdata.util.ConfigurationBL
import org.junit.Test

/**
 * Created by MK33 on 2016/3/23.
 */
@Test
class HotSaleConfTest {

  @Test
  def testAddResource = {
    val hotSaleConf = new ConfigurationBL()
    hotSaleConf.parseConfFile("hot-sale.xml")
    assert(hotSaleConf.get("hot.sale.input.path") == "D:\\2016-03-21\\user_order_raw_data")
    assert(hotSaleConf.get("hot.sale.output") == "redis")
    assert(hotSaleConf.get("day.before.today") == "1")
  }
}
