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

    val jedisCluster = new JedisCluster(jedisClusterNodes)
    val k = jedisCluster


    jedisCluster.close()


  }
}
