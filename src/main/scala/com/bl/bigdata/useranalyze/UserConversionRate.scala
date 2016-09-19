package com.bl.bigdata.useranalyze

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.util.SparkFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.json.{JSONArray, JSONObject}

/**
  * Created by MK33 on 2016/7/20.
  */
object UserConversionRate {

  def main(args: Array[String]) {


    val sc = SparkFactory.getSparkContext("conversion rate")
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(TableInputFormat.INPUT_TABLE, "rcmd_user_view")
    hBaseConf.set("hbase.zookeeper.quorum", "slave23.bl.bigdata,slave24.bl.bigdata,slave21.bl.bigdata,slave22.bl.bigdata,slave25.bl.bigdata")
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    Logger.getRootLogger.setLevel(Level.WARN)
    val familyBytes = Bytes.toBytes("recommend")
    val viewBytes = Bytes.toBytes("record")
    val pcglBytes = Bytes.toBytes("pcgl")
    val gylBytes = Bytes.toBytes("gyl")

    val hBaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
//
//
    val records = hBaseRDD.map { case (k, row) =>
      val key = new String(row.getRow).split("_")
      if (key.length != 3) null else
      (key(0), key(1), key(2), if (row.containsColumn(familyBytes, viewBytes)) new String(row.getValue(familyBytes, viewBytes)) else null,
      if (row.containsColumn(familyBytes, pcglBytes)) new String(row.getValue(familyBytes, pcglBytes)) else null,
      if (row.containsColumn(familyBytes, gylBytes)) new String(row.getValue(familyBytes, gylBytes)) else null)
    }.filter(_ != null)
    records.cache()

    // 随机打印几条出来看看
    println("=============  records  ==============")
    records.takeSample(true, 10).foreach(println)
    println("=============   records =============")


    val viewRecords = records.map(s => (s._1, s._2, s._3, s._4)).filter(_._4 != null).map { case (channel, member, date, view) =>
        val sdf = new SimpleDateFormat("yyyyMMdd")
        val json = new JSONObject(view)
        val actType = json.getString("actType")
        val goods = json.getString("goodsId")
      ((member, sdf.format(new Date(date.toLong))), goods)
    }.distinct()

    println("===========  view  ===========")
    println(viewRecords.count())
    viewRecords.takeSample(true, 10).foreach(println)
    println("===========  view  ===========")




    val pcglRecords = records.map(s => (s._1, s._2, s._3, s._5)).filter(_._4 != null).map { case (channel, memeber, date, pcgl) =>

        val sdf = new SimpleDateFormat("yyyyMMdd")
        val jsonArray = new JSONArray(pcgl)
        val length = jsonArray.length()
        for (i <- 0 until length) yield {
          val j = jsonArray.getJSONObject(i)
          ((memeber, sdf.format(new Date(date.toLong))), j.getString("sid"))
        }
    }.flatMap(s=>s).distinct()

    println("===========  pcgl  ===========")
    pcglRecords.takeSample(true, 10).foreach(println)
    println("===========  pcgl  ===========")

    val gylRecords = records.map(s=> (s._1, s._2, s._3, s._6)).filter(_._4 != null).map { case (channel, member, date, gyl) =>
        val sdf = new SimpleDateFormat("yyyyMMdd")
        val jSONArray = new JSONArray(gyl)
        val length = jSONArray.length()
        for (i <- 0 until length) yield {
          val j = jSONArray.getJSONObject(i)
          ((member, sdf.format(new Date(date.toLong))), j.getString("sid"))
        }
    }.flatMap(s => s).distinct()


    val pgcl = pcglRecords.count()
    val gyl = gylRecords.count()

    val viewPCGL = viewRecords.join(pcglRecords).filter(s => s._2._1 == s._2._1).count()
    val viewGYL = viewRecords.join(gylRecords).filter(s=> s._2._1 == s._2._2).count()

    println("=============================")
    println(s"pgcl:   $pgcl")
    println(s"gyl:  $gyl")
    println("viewPCGL:  " + viewPCGL)
    println("viewGYL:  " + viewGYL)
    println(s"   ###  ${(viewGYL + viewPCGL).toDouble / (pgcl + gyl)}  ###")
    println("=============================")

    sc.stop()


  }

}
