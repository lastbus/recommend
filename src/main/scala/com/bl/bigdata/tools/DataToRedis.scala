package com.bl.bigdata.tools

import com.bl.bigdata.util.RedisUtil._
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.JedisPool

/**
  * Created by blemall on 3/30/16.
  * only for import data to redis server.Many scenes like this in future
  *
  */
object DataToRedis {
    val MEMBER_COOKIE = "member_cookie_"
    def main (args: Array[String]) {
        val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        val lines = sc.textFile("/home/blemall/workspace/member_id_cookie_id")
        val rdd = lines.filter(r => r != null).map { r =>
            val parts = r.split("\t")
            (parts(0), parts(1))
        }.distinct().reduceByKey(_ + "#" + _).collect().toMap
        val jedisPool = getJedisPool
        saveToRedis(sparkConf, jedisPool, rdd)
    }

    def saveToRedis(sparkConf: SparkConf ,jedisPool: JedisPool, values: Map[String, String]): Unit = {
        sparkConf.set("redis.host", "10.201.128.216")
        sparkConf.set("redis.port", "6379")
        sparkConf.set("redis.timeout", "10000")
        val jedis = jedisPool.getResource
        values.map{
            v =>
                println(v)
                jedis.set(MEMBER_COOKIE + v._1, v._2)
        }

    }
}
