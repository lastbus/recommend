package bl.testing.mess

import java.util

import org.junit.Assert._
import org.junit._
import redis.clients.jedis.{Jedis, HostAndPort, JedisCluster}

@Test
class AppTest {

    @Test
    def testOK() = assertTrue(true)


    @Test
    def cluster = {

        val jedis = new Jedis("10.201.128.216")
        val key = jedis.keys("*")

        val hostAndPortSet = new util.HashSet[HostAndPort]()
        hostAndPortSet.add(new HostAndPort("10.201.129.74", 6379))
        hostAndPortSet.add(new HostAndPort("10.201.129.80", 7000))
        hostAndPortSet.add(new HostAndPort("10.201.129.75", 7000))
        val cluster = new JedisCluster(hostAndPortSet)

        cluster
//        println(cluster.get("rcmd_cookieid_view_61488111077114621098051"))
//        println(cluster.get("rcmd_cookieid_view_61488111077114621098054"))
//        println(cluster.get("rcmd_cookieid_view_61488111077114621098053"))
//        println(cluster.get("rcmd_cookieid_view_61488111077114621098052"))
        cluster.set("write_from_slave", "test")


        println("=======")
    }

}


