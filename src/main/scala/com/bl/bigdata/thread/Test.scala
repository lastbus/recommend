package com.bl.bigdata.thread

import java.util.concurrent.CountDownLatch

/**
 * Created by MK33 on 2016/6/29.
 */
object Test {

  def main(args: Array[String]) {
    var i = 1
    while (true) {
      println(s"i = $i")
      new Thread(new HoldThread()).start()
      i += 1
    }
  }

}

class HoldThread() extends Thread {
  val countDownLath = new CountDownLatch(1)

  this.setDaemon(true)

  override def run() = {
    try {
      countDownLath.await()
    } catch {
      case e: Exception => println(e.getMessage)
    }

  }

}
