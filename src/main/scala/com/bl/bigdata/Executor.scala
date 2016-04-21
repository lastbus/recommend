package com.bl.bigdata

import com.bl.bigdata.util.SparkFactory

/**
 * Created by MK33 on 2016/4/19.
 */
object Executor {

  def main (args: Array[String]){
    val sc = SparkFactory.getSparkContext("executors")

    for (c <- sc.getConf.getAll) println("key:" + c._1 + "\tvalue:" + c._2)

    println(sc.getConf.get("spark.executor.instances"))
    println(sc.getConf.get("spark.executor.cores"))


  }

}
