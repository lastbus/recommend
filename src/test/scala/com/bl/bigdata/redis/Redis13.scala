package com.bl.bigdata.redis

import redis.clients.jedis.Jedis

/**
 * Created by MK33 on 2016/5/20.
 */

object Redis13 {

  def main(args: Array[String]) {

    val jedis = new Jedis("10.201.48.13")
    jedis.set("test", "test")
    jedis.close()

  }
}
