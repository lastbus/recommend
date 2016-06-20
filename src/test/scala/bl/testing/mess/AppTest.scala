package bl.testing.mess

import java.net.InetAddress

import org.junit._
import Assert._

@Test
class AppTest {

    @Test
    def testOK() = assertTrue(true)

//    @Test
//    def testKO() = assertTrue(false)


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

}


