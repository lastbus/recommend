package bl.testing.mess.logging

/**
 * Created by MK33 on 2016/5/20.
 */
object Excc {

  def main(args: Array[String]) {
    try {
      val g = mkexe()
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

  def mkexe(): Unit ={
    val ex = try { 1 /0 } catch {
      case e: Exception =>
        println(e.getMessage)
        throw new Exception("dfjsdf" + e.getMessage)
    }
  }
}
