package com.bl.bigdata.util

import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.{Protocol, JedisPoolConfig, JedisPool, Jedis}

/**
  * Created by MK33 on 2016/3/21.
  */
object RedisTest {

  def main(args: Array[String]): Unit = {
//    jedis.auth("")

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("redis")
    val sc = new SparkContext(sparkConf)

    val j = getJedisPool
    val rawRDD = sc.textFile("D:\\2\\useruser_behavior_raw_data")
      .map(line => {
        val jedis = j.getResource
        jedis.set("user_behavior", line)
        jedis.set("m", "m")
        jedis.close()
      }).count()
    j.destroy()
    println(rawRDD)

  }

  def getJedisPool = {
    new JedisPool(new JedisPoolConfig, "", 6379, Protocol.DEFAULT_TIMEOUT, "") with Serializable
  }

}
