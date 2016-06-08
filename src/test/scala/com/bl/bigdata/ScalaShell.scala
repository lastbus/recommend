package com.bl.bigdata

import java.io.{BufferedReader, InputStreamReader}

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

    val out = new BufferedReader(new InputStreamReader(p.getErrorStream))
    var in = out.readLine()
    while (in != null)
    {
      println(in)
      in = out.readLine()
    }

    println("===========   end   ============")
  }
}
