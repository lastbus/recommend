package com.bl.bigdata.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Created by MK33 on 2016/4/7.
  */
object RedisClient extends Serializable {
  val redisHost = "10.201.128.216"
  val redisPort = 6379
  val redisTimeout = 30000
  val conf = new GenericObjectPoolConfig
  conf.setMaxTotal(100)
  lazy val pool = new JedisPool(conf, redisHost, redisPort, redisTimeout)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}
