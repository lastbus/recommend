package com.bl.bigdata.redis

/**
 * Created by MK33 on 2016/5/20.
 */
class InitRedisError(val host: String) extends RedisMessage {
  override def toString(): String = host
}
