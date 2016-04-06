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
<<<<<<< Updated upstream
//    ConfigurationBL.parseConfFile("hot-sale.xml")
//    //assert(ConfigurationBL.get("hot.sale.input.path") == "D:\\2016-03-21\\user_order_raw_data")
//    assert(ConfigurationBL.get("hot.sale.output") == "redis")
//    assert(ConfigurationBL.get("day.before.today") == "1")
=======
    ConfigurationBL.addResource("hot-sale.xml")
    assert(ConfigurationBL.get("hot.sale.input.path") == "D:\\2016-03-21\\user_order_raw_data")
    assert(ConfigurationBL.get("hot.sale.output") == "redis")
    assert(ConfigurationBL.get("day.before.today") == "1")
>>>>>>> Stashed changes
  }
}
