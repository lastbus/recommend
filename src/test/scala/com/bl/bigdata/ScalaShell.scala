package com.bl.bigdata

import java.io.{FileInputStream, File, BufferedReader, InputStreamReader}
import java.lang.ProcessBuilder.Redirect
import java.util.Properties
import scala.collection.JavaConversions._

import org.junit.Test

/**
 * Created by MK33 on 2016/6/8.
 */
@Test
class ScalaShell
{

  @Test
  def test =
  {
    println("=========    start   ==============")
    val runtime = Runtime.getRuntime
    val p = runtime.exec("test.bat")

    val out = new BufferedReader(new InputStreamReader(p.getInputStream))
    var in = out.readLine()
    while (in != null)
    {
      println(in)
      in = out.readLine()
    }

    println("===========   end   ============")
  }

  @Test
  def test2 =
  {
    val process = new ProcessBuilder("test.bat")
//    val p = process.command("echo Hello world")

//    process.directory(new File("myDir"))
    val log = new File("log.txt")
    process.redirectErrorStream(true)
    process.redirectOutput(Redirect.appendTo(log))
    val p = process.start()

    val out = new BufferedReader(new InputStreamReader(p.getInputStream))
    var in = out.readLine()
    while (in != null)
    {
      println(in)
      in = out.readLine()
    }

  }

  @Test
  def test3 = {

    val runtime = Runtime getRuntime

    Console println ( runtime availableProcessors )
    Console println ( runtime freeMemory )
    System.exit(-1)


  }

  @Test
  def test4 = {
    val p = System.getenv()
    val itera = p.keySet().iterator()
    while (itera.hasNext){
      val key = itera.next()
      println(key + " :: " + p.get(key))
    }
    val propsStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("log.txt")
    if (propsStream == null) println("====== null ======")
    val in = new FileInputStream("log.txt")

  }

  @Test
  def test5 = {
    val propsStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("test.props")
    val props = new Properties()
    props.load(propsStream)
    props.keySet().foreach(k => println(k +  "  " +  props.get(k)))



  }


  @Test
  def test6 = {

    val clazz = Class.forName("com.bl.ArgsTest")
    val args = "com.bl.ArgsTest -i input -o output out2 -Dxmx=100m".split(" ")
    val main = clazz.getMethod("main", classOf[Array[String]])
    main.invoke(null, args)

    println("end")

  }



}
