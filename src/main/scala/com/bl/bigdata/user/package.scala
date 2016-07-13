package com.bl.bigdata.user

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by HJT20 on 2016/5/17.
 */
 object test extends App{
  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  val today = sdf.format(new Date)
  val b=sdf.parse(today).getTime-sdf.parse("2016-04-18").getTime
  val num=b/(1000*3600*24)
  println(num)

}
