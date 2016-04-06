package com.bl.bigdata.ranking

import com.bl.bigdata.util.{PropertyUtil, ConfigurationBL}
import org.junit.Test

/**
 * Created by MK33 on 2016/3/23.
 */
@Test
class HotSaleConfTest {

  @Test
  def testAddResource = {
    ConfigurationBL.parseConfFile("hot-sale.xml")
    assert(PropertyUtil.get("hot.sale.input.path") == "D:\\2016-03-21\\user_order_raw_data")
    assert(PropertyUtil.get("hot.sale.output") == "redis")
    assert(PropertyUtil.get("day.before.today") == "1")
  }
}
