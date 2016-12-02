package com.bl.bigdata.util

import org.junit._

/**
  * Created by MK33 on 2016/10/26.
  */
@Test
class SqlFactoryTest {

  @Test
  def getSqlTest = {
    val sql1 =
      """
        |select u.cookie_id, u.category_sid, u.event_date, u.behavior_type, u.goods_sid, g.store_sid
        |from recommendation.user_behavior_raw_data u inner join recommendation.goods_avaialbe_for_sale_channel g on g.sid = u.goods_sid and g.sale_status = 4
        |where dt >= '0'
      """.stripMargin
    val sql2 = SqlFactory.getSql("browser", "0")
    val a1 = sql1.toArray.filter(s => s.toByte > 32 && s.toByte < 127)
    val a2 = sql2.toArray.filter(s => s.toByte > 32 && s.toByte < 127)
    assert(a1.size == a2.size)
    for (i <- 0 until a1.size) {
      assert(a1(i) == a2(i))
    }

  }


}
