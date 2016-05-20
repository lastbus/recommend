package com.bl.bigdata.redis


import com.bl.bigdata.util.ConfigurationBL
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.logging.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import redis.clients.jedis.{Jedis, JedisPool}

/**
 * Created by MK33 on 2016/5/20.
 */
object RedisWriter extends Serializable {
  val logger = LogManager.getLogger

  var jedisPool: JedisPool = _

  def getRedisConn: Jedis = new Jedis(ConfigurationBL.get("redis.host"))

  def getRedisConnPool: JedisPool = {
    require(ConfigurationBL.getAll.length > 0, "configuration object is empty!")
    if (jedisPool == null) {
      val redisConf = new GenericObjectPoolConfig
      redisConf.setMaxTotal(10)
      new JedisPool(redisConf, ConfigurationBL.get("redis.host"),
        ConfigurationBL.get("redis.port", "6379").toInt,
        ConfigurationBL.get("redis.timeout", "1000").toInt)
    } else {
      jedisPool
    }

  }

  def saveStringValue(rdd: RDD[(String, String)],
                      count: Boolean = true): String = {
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    val num = rdd.count()
    val accumulator = rdd.sparkContext.accumulator(0)
    rdd.foreachPartition { partition =>
      ConfigurationBL.addResource("recmd-conf.xml")
      val jedisPool =
        try {
          getRedisConnPool.getResource
        } catch {
          case e: Exception => logger.fatal(e.getMessage)
            throw new Exception("get redis connection failed: " + e.getMessage)
        }

      // 是累计一定数量的失败才抛出异常还是一发生异常就抛出呢？
      for (kv <- partition) {
        try {
          jedisPool.set(kv._1, kv._2)
          accumulator += 0
        } catch {
          case e: Exception => logger.error(e.getCause)
            throw new Exception(s"redis error key: ${kv._1} : ${e.getMessage}")
        }
      }

    }
    // 返回执行结果的统计信息
    val msg =
      s"""redis success message:
         |   host: ${ConfigurationBL.get("redis.name")}
         |   port: ${ConfigurationBL.get("redis.port", "6379")}
         |   timeout: ${ConfigurationBL.get("redis.timeout", "1000")}
         |   in: $num
         |   success: $accumulator
    """.stripMargin
    msg
  }

  /**
   * 保存 hash 值
   * @param rdd
   * @return
   */
  def saveHashValue(rdd: RDD[(String, java.util.HashMap[String, String])]): String = {
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    val num = rdd.count()
    val accmulator = rdd.sparkContext.accumulator(0)
    rdd.foreachPartition { partition =>
      ConfigurationBL.addResource("recmd-conf.xml")
      val jedisPool =
        try {
          getRedisConnPool.getResource
        } catch {
          case e: Exception => logger.fatal(e.getMessage)
            throw new Exception("get redis connection failed:" + e.getMessage)
        }

      // 是累计一定数量的失败才抛出异常还是一发生异常就抛出呢？
      for (kv <- partition) {
        try {
          jedisPool.hmset(kv._1, kv._2)
          accmulator += 0
        } catch {
          case e: Exception => logger.error(e.getMessage)
            throw new Exception(s"redis error key: ${kv._1} : ${e.getMessage}")
        }
      }
    }
    // 返回执行结果的统计信息
    val msg =
      s"""redis success message:
         |   host: ${ConfigurationBL.get("redis.name")}
         |   port: ${ConfigurationBL.get("redis.port", "6379")}
         |   timeout: ${ConfigurationBL.get("redis.timeout", "1000")}
         |   in: $num
         |   success: $accmulator
    """.stripMargin
    msg
  }

  
}
