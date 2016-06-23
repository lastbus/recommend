package com.bl.bigdata.shell

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.util.SparkFactory
import org.apache.spark.sql.hive.HiveContext
/**
 * Created by MK33 on 2016/6/23.
 */
object hive {

  def main(args: Array[String]): Unit = {
    val sc = SparkFactory.getSparkContext("hive")


    val hive = new HiveContext(sc)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val startDate = sdf.parse("20150501")
    val startTime = startDate.getTime
    val endDate = sdf.parse("20170501")
    val endTime = endDate.getTime
    val length = (endTime - startTime) / 1000 / 3600 / 24
    val arrayBuffer = new Array[String](length.toInt + 1)
    var i = 0
    while (i <= length){
      arrayBuffer(i) = sdf.format(new Date(startTime + 24L * 3600 * 1000 * i))
      i += 1
    }


    import hive.implicits._
    sc.parallelize(arrayBuffer.toSeq).toDF.registerTempTable("date")
    hive.sql("insert overwrite table idmdata.dim_period select * from date")

  }


}
