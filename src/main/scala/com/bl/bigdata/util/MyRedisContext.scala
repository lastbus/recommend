package com.bl.bigdata.util

import com.redislabs.provider.redis.{RedisEndpoint, RedisConfig, RedisContext}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import Implicts._
import redis.clients.jedis.{JedisPoolConfig, JedisPool}

/**
  * Created by MK33 on 2016/3/25.
  */
class MyRedisContext(sc: SparkContext) extends RedisContext(sc) {

  import MyRedisContext._

  def hashKVRDD2Redis(kvs: RDD[(String, Map[String, String])], ttl: Int = 0)
                     (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))): Unit = {
    kvs.foreachPartition(partition => setHashKVs(partition, ttl, redisConfig))

  }
}

object MyRedisContext {

  /**
    * @param arr k/vs which should be saved in the target host
    *            save all the k/vs to the target host
    * @param ttl time to live
    */
  def setHashKVs(arr: Iterator[(String, Map[String, String])], ttl: Int, redisConfig: RedisConfig) {
    arr.map(kv => (redisConfig.getHost(kv._1), kv)).toArray.groupBy(_._1).
      mapValues(a => a.map(p => p._2)).foreach {
      x => {
        val conn = x._1.endpoint.connect()
        val pipeline = x._1.endpoint.connect.pipelined
        if (ttl <= 0) {
          x._2.foreach(x => pipeline.hmset(x._1, x._2))
        }
        else {
          x._2.foreach(x => pipeline.expire(x._1, ttl))
        }
        pipeline.sync
        conn.close
      }
    }
  }

  implicit def toMyRedisContext(sc: SparkContext): MyRedisContext = new MyRedisContext(sc)
}

object JedisPoolTest {

  def main(args: Array[String]) {
    val jedisPool = new JedisPool(new JedisPoolConfig, "localhost")
    import com.bl.bigdata.util.Implicts._
    val jedis = jedisPool.getResource
//    jedis.set("jedis", "not returned back jedis.")
    val map = Map("a" -> "1", "b" -> "2")
//    jedis.hmset("map", map)
    jedis.del("map")
    jedis.close()
    jedisPool.destroy()
  }
}

