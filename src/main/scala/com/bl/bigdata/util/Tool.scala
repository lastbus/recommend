package com.bl.bigdata.util

import org.apache.spark.rdd.RDD

/**
  * Created by MK33 on 2016/3/10.
  */
abstract class Tool extends Serializable {

  def run(args: Array[String])



}
