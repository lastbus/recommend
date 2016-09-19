package bl.testing.mess

import java.util
import org.junit.Assert._
import java.net.InetAddress
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


    @Test
    def similarity = {
        val d1 = Map(1 -> 0.2, 2 -> 0.5, 3 -> 0.8)
        val d2 = Map(1 -> 0.2, 2 -> 0.5, 3 -> 0.8)
        var sum = 0.0
        var divider1 = 0.0
        d1.foreach { case (k, v) =>
            val tmp = d2.get(k)
            if (!tmp.isEmpty) sum +=  (v * tmp.get)
            println(sum)
            divider1 += v * v
        }

        val divider2 = d2.map(s => s._2 * s._2).sum

        val similarity = sum / (scala.math.sqrt(divider1) * scala.math.sqrt(divider2))

        println(divider1)
        println(divider2)
        println(similarity)


    }


    @Test
    def test0 = {
        val address = InetAddress.getByName("localhost")
        println(InetAddress.getLocalHost.getCanonicalHostName)


    }

    @Test
    def doub = {

        val d = "1.1.1"
        println(d.toDouble)


    }

}


