package com.bl.bigdata.util

import java.util
import java.util.Properties

import scala.collection.JavaConversions._
import com.bl.bigdata.mail.Message
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.logging.log4j.LogManager
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPool}


/**
  * Created by MK33 on 2016/4/7.
  */
object RedisClient extends Serializable {
  val logger = LogManager.getLogger(this.getClass.getName)
  val cluster: String = "cluster"
  val standalone: String = "standalone"

  lazy val confProperties = {
    val properties = new Properties()
    loadProperties(properties, "conf.properties")
    properties
  }

  lazy val pool = {
    val fileName = confProperties.getProperty("redis.conf.name.standalone")
    if (fileName == null) {
      println("cannot find <redis.conf.name.standalone>")
      sys.exit(-1)
    }
    val conf = new GenericObjectPoolConfig
    conf.setMaxTotal(100)
    val properties = new Properties()
    loadProperties(properties, fileName)
    val host = properties.getProperty("redis.host")
    val port = properties.getProperty("redis.port").toInt
    val timeout = properties.getProperty("redis.timeout").toInt
    sys.addShutdownHook(jedisHook.run())
    new JedisPool(conf, host, port, timeout)
  }

  lazy val jedisCluster = {
    val fileName = confProperties.getProperty("redis.conf.name.cluster")
    if (fileName == null) {
      println("cannot find <redis.conf.name.cluster>.")
      sys.exit(-1)
    }
    val properties = new Properties()
    loadProperties(properties, fileName)
    val sets = new util.HashSet[HostAndPort]()
    for (key <- properties.stringPropertyNames()) {
      val value = properties.getProperty(key).split(":")
      sets.add(new HostAndPort(value(0), value(1).toInt))
    }
    sys.addShutdownHook(jedisClusterHook2.run())
    new JedisCluster(sets)
  }

  /** export key-value pair to redis */
  def sparkKVToRedis(kv: RDD[(String, String)], accumulator: Accumulator[Int], redisType: String): Unit = {
    kv.foreachPartition(partition => {
      try {
        redisType match {
          case RedisClient.cluster =>
            partition.foreach { s =>
              jedisCluster.set(s._1, s._2)
              accumulator += 1
            }
          case RedisClient.standalone =>
            val jedis = pool.getResource
            partition.foreach { s =>
              jedis.set(s._1, s._2)
              accumulator += 1
            }
            jedis.close() // must release connection resource !
          case _ => logger.error(s"wrong redis type $redisType ")
        }
      } catch {
        case e: Exception => Message.addMessage(e.getMessage)
      }
    })
  }

  /** export key-value pair to redis */
  def sparkKVToRedis(kv: RDD[(String, String)], accumulator: Accumulator[Int], redisType: String, expireTime: Int): Unit = {
    kv.foreachPartition(partition => {
      try {
        redisType match {
          case RedisClient.cluster =>
            partition.foreach { s =>
              jedisCluster.setex(s._1, expireTime, s._2)
              accumulator += 1
            }
          case RedisClient.standalone =>
            val jedis = pool.getResource
            partition.foreach { s =>
              jedis.setex(s._1, expireTime, s._2)
              accumulator += 1
            }
            jedis.close() // must release connection resource !
          case _ => logger.error(s"wrong redis type $redisType ")
        }
      } catch {
        case e: Exception => Message.addMessage(e.getMessage)
      }
    })
  }


  /** export hash value to redis */
  def sparkHashToRedis(kv: RDD[(String, util.HashMap[String, String])], accumulator: Accumulator[Int], redisType: String): Unit = {
    kv.foreachPartition(partition => {
      try {
        redisType match {
          case RedisClient.cluster =>
            partition.foreach { s =>
              jedisCluster.hmset(s._1, s._2)
              accumulator += 1
            }
          case RedisClient.standalone =>
            val jedis = pool.getResource
            partition.foreach { s =>
              jedis.hmset(s._1, s._2)
              accumulator += 1
            }
            jedis.close() // must release connection resource !
          case _ => logger.error(s"wrong redis type $redisType ")
        }
      } catch {
        case e: Exception => Message.addMessage(e.getMessage)
      }
    })
  }

  /** export hash value to redis */
  def sparkHashToRedis(kv: RDD[(String, util.HashMap[String, String])], accumulator: Accumulator[Int], redisType: String, ttl: Int): Unit = {
    kv.foreachPartition(partition => {
      try {
        redisType match {
          case RedisClient.cluster =>
            partition.foreach { s =>
              jedisCluster.hmset(s._1, s._2)
              jedisCluster.expire(s._1, ttl)
              accumulator += 1
            }
          case RedisClient.standalone =>
            val jedis = pool.getResource
            partition.foreach { s =>
              jedis.hmset(s._1, s._2)
              jedis.expire(s._1, ttl)
              accumulator += 1
            }
            jedis.close() // must release connection resource !
          case _ => logger.error(s"wrong redis type $redisType ")
        }
      } catch {
        case e: Exception => Message.addMessage(e.getMessage)
      }
    })
  }


  val jedisHook: Thread = new Thread {
    override def run() = {
      logger.info("==========  shutdown jedis connection  =================")
      if (pool != null) pool.destroy()
    }
  }

  val jedisClusterHook2: Thread = new Thread {
    override def run() = {
      logger.info("=======  shutdown jedis cluster connection  ============")
      if (jedisCluster != null) jedisCluster.close()
    }
  }


  def loadProperties(props: Properties, fileName: String): Unit = {
    val in = this.getClass.getClassLoader.getResourceAsStream(fileName)
    if (in == null) return
    try {
      props.load(in)
    } finally {
      in.close()
    }
  }


}
