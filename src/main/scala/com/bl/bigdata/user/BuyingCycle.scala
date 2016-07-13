package com.bl.bigdata.user

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.util.SparkFactory
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/5/26.
 */
class BuyingCycle {

  def categoryBuyingCycle(): Unit = {
    val sc = SparkFactory.getSparkContext("category buying cycle")
    val hiveContext = new HiveContext(sc)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * 60))
    val sql = "select  member_id, event_date, category_sid from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start and behavior_type=4000 and member_id is not null"
    //category_sid,member_id,date
    val cbcRawRdd  = hiveContext.sql(sql).rdd.map(row=>((row.getString(2),row.getString(0)),(row.getString(1).substring(0,10))))
    val udcRdd = cbcRawRdd.mapValues(x=>Seq(x)).reduceByKey( _ ++ _).mapValues(x=>x.distinct.sorted).filter(_._2.size>1)
    val ccsRdd = udcRdd.mapValues(x=>
    {
      val ax = x.toArray
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val cycle = for(i<-0 until  ax.length-1) yield (sdf.parse(ax(i+1)).getTime-sdf.parse(ax(i)).getTime)/(1000*3600*24)
      cycle
    }).map(x=>{(x._1._1,x._2)}).reduceByKey(_++_)
    val cmedRdd = ccsRdd.mapValues(x=>{
      val sx = x.sorted
      val count = sx.length
      val median: Double = if (count % 2 == 0) {
        val l = count / 2 - 1
        val r = l + 1
        (sx(l) + sx(r)).toDouble / 2
      } else sx(count / 2).toDouble
      (median,count)
    })
    cmedRdd.foreach(println)

    // conf.addResource("hbase-site.xml")
    cmedRdd.foreachPartition{
      x=> {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        conf.set("hbase.zookeeper.quorum", "10.201.129.81")
        conf.set("hbase.master", "10.201.129.78:60000")
        val categoryTable = new HTable(conf, TableName.valueOf("category_buying_cycle"))
        categoryTable.setAutoFlush(false, false)
        categoryTable.setWriteBufferSize(3*1024*1024)
        x.foreach { y => {
         // println(y(0) + ":::" + y(1))
          val p = new Put(Bytes.toBytes(y._1.toString))
          p.addColumn("category".getBytes, "cycle".getBytes, Bytes.toBytes(y._2._1.toString))
          p.addColumn("category".getBytes, "observation".getBytes, Bytes.toBytes(y._2._2.toString))
          categoryTable.put(p)
        }
        }
        categoryTable.flushCommits()
      }
    }
    sc.stop()

  }
}

object BuyingCycle{

  def main(args: Array[String]) {
    val bc = new BuyingCycle
    bc.categoryBuyingCycle
  }

}
