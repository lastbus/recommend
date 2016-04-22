package com.bl.bigdata.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Created by MK33 on 2016/4/7.
  */
object RedisClient extends Serializable {
  //TODO 把 redis 的配置放在配置文件中

  val conf = new GenericObjectPoolConfig
  conf.setMaxTotal(100)
  lazy val pool = new JedisPool(conf,  "10.201.128.216",
                                        ConfigurationBL.get("redis.port", "6379").toInt,
                                        ConfigurationBL.get("redis.timeout", "10000").toInt)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}
