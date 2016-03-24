package com.bl.bigdata.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{JedisPoolConfig, JedisPool}

/**
  * Created by MK33 on 2016/3/8.
  */
object RedisUtil extends Serializable {
  val redisHost = "10.201.128.126"
  val redisPort = 6379
  val redisTimeout = 30000
  lazy val pool = new JedisPool(new GenericObjectPoolConfig, redisHost, redisPort, redisTimeout)


  lazy val hook = new Thread{
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }

  sys.addShutdownHook(hook.run)


  def getJedisPool = {
    new JedisPool(new JedisPoolConfig, "10.201.128.216") with Serializable
  }
}

