package com.bl.bigdata.redis

import java.util

import redis.clients.jedis.{JedisCluster, HostAndPort}

/**
 * Created by MK33 on 2016/5/20.
 */

object Redis13 {

  def main(args: Array[String]) {

    val jedisClusterNodes = new util.HashSet[HostAndPort]()
    jedisClusterNodes.add(new HostAndPort("10.201.129.74", 6379))
//    jedisClusterNodes.add(new HostAndPort("10.201.129.75", 6379))
//    jedisClusterNodes.add(new HostAndPort("10.201.129.80", 6379))

//    val r = "rcmd_cate_hotsale_.*_.*".r.pattern
    val jedisCluster = new JedisCluster(jedisClusterNodes)
    val ks = jedisCluster.getClusterNodes
//    var  i = 0
    import scala.collection.JavaConversions._
    for (k <- ks.keySet()){
//      val kk = ks.get(k).getResource.keys("rcmd_view_*")
//      val kk = ks.get(k).getResource.keys("rcmd_shop_*")
//      val kk = ks.get(k).getResource.keys("rcmd_bab_goods_*")
//      val kk = ks.get(k).getResource.keys("rcmd_cate_hotsale_*")
//      val kk = ks.get(k).getResource.keys("rcmd_cate_goods_*")
        val kk = ks.get(k).getResource.keys("rcmd_cate_goods_*")
      for (k00 <- kk){
        jedisCluster.del(k00)
      }
    }
    jedisCluster.close()


  }
}
