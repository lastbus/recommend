package com.bl.bigdata.redis

import redis.clients.jedis.Jedis

/**
  * Created by MK33 on 2016/4/5.
  */
object Test {

  def main(args: Array[String]) {


    val jedis = new Jedis("10.201.128.216")
    val all = jedis.keys("rcmd_orig_pc_*").toArray
    println(all.length)
//    val t = jedis.multi()
    all.foreach{ s =>
      jedis.del(s.toString)
    }
//    t.exec()


//    val request = "http://10.201.128.216:8080/recommend/view?gId=188691"
//    val url = new URL(request)
//    val res = url.openConnection()
//    val r = res.getInputStream

  }


}
