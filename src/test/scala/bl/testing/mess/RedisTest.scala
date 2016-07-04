package bl.testing.mess

import redis.clients.jedis.{HostAndPort, JedisCluster}

/**
  * Created by MK33 on 2016/3/21.
  */
object RedisTest {

  def main(args: Array[String]): Unit = {

    val set1 = new java.util.HashSet[HostAndPort]()
    set1.add(new HostAndPort("s74sit", 7000))
    set1.add(new HostAndPort("s74sit", 6379))
    set1.add(new HostAndPort("s75sit", 7000))
    set1.add(new HostAndPort("s75sit", 6379))
    set1.add(new HostAndPort("s80sit", 7000))
    set1.add(new HostAndPort("s80sit", 6379))

    val jedisCluster = new JedisCluster(set1)
    for (i <- 0 to 10){
      if (jedisCluster.exists("count0"))
        jedisCluster.incr("count0")
      else {
        jedisCluster.setex("count0", 20, "0")
        jedisCluster
      }

    }

    jedisCluster.close()





  }


}
