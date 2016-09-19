package com.bl.bigdata.machinelearning

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.util.{RedisClient, SparkFactory}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/6/13.
 */
class ItemCF {
  def usertaste(day:Int): Unit = {
    val sc = SparkFactory.getSparkContext("item_cf")
    val hiveContext = new HiveContext(sc)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * day))
    val sql = "select member_id, event_date, behavior_type, category_sid, goods_sid from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start and behavior_type=1000 and member_id is not null and  member_id <>'null' and member_id <>'NULL' and member_id <>''"
    //ck,date,category,goods_sid=>1
    val ucgRawRdd = hiveContext.sql(sql).rdd.map(row => ((row.getString(0), row.getString(1).substring(0, 10), row.getString(3),
      row.getString(4)), 1))
    //category,goods,date=>memId,pv
    val ucAcuRdd = ucgRawRdd.reduceByKey(_ + _).map(x => ((x._1._3, x._1._4, x._1._2), (x._1._1, x._2)))
    val goodsSQL = "select category_sid, goods_sid,date,pv from recommendation.user_preference_goods_visit"
    //category,goods,date,count pv
    val cgRdd = hiveContext.sql(goodsSQL).rdd.map(row => ((row.getString(0), row.getString(1), row.getString(2)), row.getLong(3)))
    //((102564,goodsID,2016-04-16),((88867259525614535970958,1),3))
    val unormRdd = ucAcuRdd.join(cgRdd)
    //((103466,(21021460629373192854469,4)),0.18402591023557582,33,2016-04-14)
    val prefRdd = unormRdd.map(x=>{
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val today = sdf.format(new Date)
      val b=sdf.parse(today).getTime-sdf.parse(x._1._3).getTime
      val num=b/(1000*3600*24)
      //(goods_sid,mId)
      ((x._1._1,x._1._2,x._2._1._1),((x._2._1._2.toDouble/x._2._2)*scala.math.pow(1,num)))})
      .reduceByKey( _  + _).filter(_._2 > 0.0001)
      .map(u=>{
        //mId,cateID,
        ((u._1._3,u._1._1),Seq((u._1._2,u._2)))
      }).reduceByKey(_ ++ _)
      .mapValues(x=>{
        x.sortWith((a,b)=>a._2>b._2)
      })
    //((100000000019135,103071),List((264363,0.0010033912775533338), (268533,0.0010033912775533338)))
    .flatMap(x=>{
      val mId=x._1._1
      x._2.take(30).map(y=>(mId,(y._1,y._2)))

    })
   // prefRdd.saveAsTextFile("/home/hdfs/prefRdd")

//    val purRdd = prefRdd.map { case (mId, (pId, rating)) =>
//      (pId, Map(mId -> rating.toDouble))
//    }.reduceByKey(_ ++ _)


    val purRdd2 = prefRdd.map { case (mId, (pId, rating)) =>
      (pId, (mId, rating.toDouble))
    }

    val purRdd = purRdd2.mapValues{case(mId,rating)=>Map(mId->rating)}.reduceByKey(_ ++ _)


    val ppRdd = purRdd.cartesian(purRdd)
    val tmpUpRdd = ppRdd.map {
      case (ud1, ud2) =>
        val id1 = ud1._1
        val id2 = ud2._1
        val d1 = ud1._2
        val d2 = ud2._2
        var sum = 0.0
        var divider1 = 0.0
        d1.foreach { case (k, v) => {
          val tmp = d2.get(k)
          if (!tmp.isEmpty) sum += (v * tmp.get)
          divider1 += v * v
        }
        }
        val divider2 = d2.map(s => s._2 * s._2).sum
        (id1, (id2, sum / (scala.math.sqrt(divider1) * scala.math.sqrt(divider2))))
    }.filter(s => s._2._2.toDouble > 0.000001 && s._1 != s._2._1).mapValues(v => Seq(v))//距离小还是大
      .reduceByKey(_ ++ _).distinct().mapValues(v => v.sortWith((a, b) => a._2 > b._2)).mapValues(v => v.take(10)).cache()

   // tmpUpRdd.saveAsTextFile("/home/hdfs/itecf")

    val pupRdd = purRdd2.join(tmpUpRdd)
      .flatMap{x=>{
      val uId = x._2._1._1
      val deg = x._2._1._2.toDouble
      x._2._2.map{case(pId,rate)=>
      {
        (uId,(pId,rate*deg))
      }}
    }}.mapValues(pd=>Seq(pd)).reduceByKey(_ ++ _).mapValues(v => v.sortWith((a, b) => a._2 > b._2).take(60))
      .map(v => {

                val pds = v._2
                val spds = pds.map(x =>
                  x._1
                ).distinct.mkString("#")
                val mId = v._1
                (mId, spds)
              }
      )
   // pupRdd.saveAsTextFile("/home/hdfs/pupRdd")

    pupRdd.foreachPartition(partition => {
      try {
        val jedisCluster = RedisClient.jedisCluster
        partition.foreach(data => {
          jedisCluster.set("rcmd_itemcf_"+data._1, data._2)
        })
        //        jedisCluster.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }

    })
    sc.stop()
  }
}

object ItemCF {
  def main(args: Array[String]) {
    val day = args(0).toInt
    val itcf = new ItemCF
    itcf.usertaste(day)
  }
}
