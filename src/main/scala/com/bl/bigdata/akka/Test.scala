package com.bl.bigdata.akka

import akka.actor.{Props, ActorSystem, Actor}

/**
 * Created by MK33 on 2016/5/25.
 */
object Test {

  def main(args: Array[String]) {
    val system = ActorSystem("test")
    val helloActor = system.actorOf(Props[HelloActor], "hello")
//    while (true)
//    {
//      val in = Console.readLine()
//      helloActor ! in
//    }
    helloActor ! "testttt"
    println("hello")
  }
}


class HelloActor extends Actor {
  override def receive: Receive = {
    case "stop" =>
      println("stop")
      sys.exit(0)
    case e => println("input: %s".format(e))
  }
}