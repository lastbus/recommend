package com.bl.bigdata.util

import java.util
import java.util.Properties

import scala.collection.JavaConversions._
import com.bl.bigdata.mail.Message
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPool}


/**
  * Created by MK33 on 2016/4/7.
  */
object RedisClient extends Serializable with Logging with Redis {

  val conf = new GenericObjectPoolConfig
  conf.setMaxTotal(100)
  lazy val pool = new JedisPool(conf, host, port, timeout)
  lazy val jedisCluster = {
    val properties = new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream("jedis-cluster.properties"))
    val sets = new util.HashSet[HostAndPort]()
    for (key <- properties.stringPropertyNames()) {
      val value = properties.getProperty(key).split(":")
      sets.add(new HostAndPort(value(0), value(1).toInt))
    }
//    set.add(new HostAndPort("10.201.129.74", 6379))
//    set.add(new HostAndPort("10.201.129.75", 6379))
//    set.add(new HostAndPort("10.201.129.80", 6379))
//    set.add(new HostAndPort("10.201.129.74", 7000))
//    set.add(new HostAndPort("10.201.129.75", 7000))
//    set.add(new HostAndPort("10.201.129.80", 7000))
    new JedisCluster(sets)
  }

  /** export key-value pair to redis */
  def sparkKVToRedis(kv: RDD[(String, String)], accumulator: Accumulator[Int], redisType: String): Unit = {
    kv.foreachPartition(partition => {
      try {
        redisType match {
          case "cluster" =>
            partition.foreach { s =>
              jedisCluster.set(s._1, s._2)
              accumulator += 1
            }
          case "standalone" =>
            val jedis = pool.getResource
            partition.foreach { s =>
              jedis.set(s._1, s._2)
              accumulator += 1
            }
            jedis.close()
          case _ => logger.error(s"wrong redis type $redisType ")
        }
      } catch {
        case e: Exception => Message.addMessage(e.getMessage)
      }
    })
  }

  val jedisHook = new Thread {
    override def run() = {
      logger.info("==========  shutdown jedis  " + this  + "  =================")
      if (pool != null) pool.destroy()
    }
  }

  val jedisClusterHook2 = new Thread {
    override def run() = {
      logger.info("=======  shutdown jedis cluster connection  ============")
      if (jedisCluster != null) jedisCluster.close()
    }
  }


  sys.addShutdownHook(jedisHook.run())
  sys.addShutdownHook(jedisClusterHook2.run())

}
