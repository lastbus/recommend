package com.bl.bigdata.machinelearning

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.util.SparkFactory
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/7/22.
 */
class WordToVector {

  def export(): Unit = {
    val sc = SparkFactory.getSparkContext("w2v_exp")
    val hiveContext = new HiveContext(sc)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * 60))
    val sql = "select member_id, event_date, behavior_type, category_sid, goods_sid from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start and behavior_type=1000 and member_id is not null and  member_id <>'null' and member_id <>'NULL' and member_id <>''"
    val actRdd = hiveContext.sql(sql).rdd.map(row => (row.getString(0), (row.getString(1),row.getString(4))))
    val corpRdd = actRdd.mapValues(v=>Seq(v)).reduceByKey(_ ++ _).filter(_ ._2.size>3).mapValues(
      x=>{
        x.sortWith((a,b)=>a._1<b._2)
    }).map(x=>{
      val gs = x._2.map(g=>g._2)
      gs.mkString(" ")
    })
    corpRdd.saveAsTextFile("/home/hdfs/word2")
    sc.stop()

  }


}

object WordToVector {

  def main(args: Array[String]) {
    val vw = new WordToVector
    vw.export()
  }
}


