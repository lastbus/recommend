package com.bl.bigdata.util

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.Rating
import redis.clients.jedis.{JedisPoolConfig, JedisPool}

/**
  * Created by MK33 on 2016/3/8.
  */
object RedisUtil extends RedisBase with Serializable {
    //Constant value
    val MEMBER_COOKIE = "member_cookie_"
    val REDIS_PREFIX="rcmd_gwyl_"


    def guessWhatYouLike_saveToRedis(sparkConf: SparkConf , values: Map[String, Array[Rating]]): Unit = {
        connect(sparkConf)
        val jedisPool = getJedisPool
        val jedis = jedisPool.getResource
        import com.bl.bigdata.util.Implicts.map2HashMap
        values.map{v =>
            val map = v._2.map{r => (r.product.toString, r.rating.toString)}.distinct.toMap
            if(map.nonEmpty) {
                jedis.hmset(REDIS_PREFIX + v._1.toString, map)
            }
        }
        println("finished saving data to redis")
    }

    def saveToRedis(sparkConf: SparkConf, values: Map[String, String]): Unit = {
        connect(sparkConf)
        val jedisPool = getJedisPool
        val jedis = jedisPool.getResource
        values.map{
            v =>
                println(v)
                jedis.set(MEMBER_COOKIE + v._1, v._2)
        }
    }

    def getJedisPool = {
        new JedisPool(new JedisPoolConfig, "10.201.128.216") with Serializable
    }
}

