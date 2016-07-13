package bl.testing.mess

import com.bl.bigdata.util.RedisClient
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
    val jedisCluster2 = RedisClient.pool.getResource
//    val j = RedisClient.pool


    val thread1 = new Thread(new ThreadDemo1(jedisCluster, 0, "="))
    val thread2 = new Thread(new ThreadDemo1(jedisCluster, 1, "-"))
    val thread3 = new Thread(new ThreadDemo1(jedisCluster, 2, "+"))
    val thread4 = new Thread(new ThreadDemo1(jedisCluster, 3, "#"))
    val thread5 = new Thread(new ThreadDemo1(jedisCluster, 4, "%"))
    val thread6 = new Thread(new ThreadDemo1(jedisCluster, 5, "&"))

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()
    thread6.start()




  }




}


class ThreadDemo1(jedisCluster: JedisCluster, threadNum: Int, star: String) extends Runnable {

  val key = "cluster"

  lazy val a: String = {
    println("================")
    "test"
  }


  sys.addShutdownHook(new Thread(){

    override def run(): Unit = {
      if (a != null) println(" not instance")
    }
  })


  override def run(): Unit = {
    println(s" Thread $threadNum begin to execute.")
    for (i <- 1 to 10){
      println(threadNum)
      if (jedisCluster.exists(key)){
        jedisCluster.incr(key)
      } else {
        jedisCluster.incr(key)
      }
    }
    println(star * 10 + s"  Thread $threadNum execute to end   " + star * 10)
  }
}


object LazyTest {

  lazy val a: String = {
    println("================")
    "test"
  }


  sys.addShutdownHook(new Thread(){

    override def run(): Unit = {
      if (a != null) println(" not instance")
    }
  })

}