package com.bl.bigdata.util

import java.util

import com.bl.bigdata.mail.Message
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{JedisCluster, HostAndPort, JedisPool}

/**
  * Created by MK33 on 2016/4/7.
  */
object RedisClient extends Serializable with Logging with Redis {


  val conf = new GenericObjectPoolConfig
  conf.setMaxTotal(100)

  lazy val pool = new JedisPool(conf, host, port, timeout)

  val hook = new Thread {
    override def run() = {
      logger.info("==========  Execute hook thread: " + this  + "  =================")
      if (pool != null) pool.destroy()
    }
  }

  val hook2 = new Thread {
    override def run() = {
      logger.info("=======  shutdown jedis cluster connection  ============")
      if (jedisCluster != null) jedisCluster.close()
    }
  }

  lazy val jedisCluster = {
    val set = new util.HashSet[HostAndPort]()
    set.add(new HostAndPort("10.201.129.74", 6379))
    set.add(new HostAndPort("10.201.129.75", 6379))
    set.add(new HostAndPort("10.201.129.80", 6379))
    set.add(new HostAndPort("10.201.129.74", 7000))
    set.add(new HostAndPort("10.201.129.75", 7000))
    set.add(new HostAndPort("10.201.129.80", 7000))

    new JedisCluster(set)
  }

  /** export key-value pair to redis */
  def sparkKVToRedis(kv: RDD[(String, String)], accumulator: Accumulator[Int], redisType: String): Unit ={
    kv.foreachPartition(partition => {
      try {
        redisType match {
          case "cluster" =>
//            val jedis = RedisClient.jedisCluster
            partition.foreach { s =>
              jedisCluster.set(s._1, s._2)
              accumulator += 1
            }
//            jedis.close()
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


  sys.addShutdownHook(hook.run())
  sys.addShutdownHook(hook2.run())



}
