package com.bl.bigdata.machinelearning

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.util.{RedisClient, SparkFactory}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.hive.HiveContext
import org.jblas.DoubleMatrix

/**
 * Created by HJT20 on 2016/6/8.
 */
class LFM {

  def userRating(day:Int): Unit = {
    val sc = SparkFactory.getSparkContext("ALS_matrix")
    val hiveContext = new HiveContext(sc)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * day))
    val sql = "select member_id, event_date, behavior_type, category_sid, goods_sid from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start and behavior_type=1000 and member_id is not null and  member_id <>'null' and member_id <>'NULL' and member_id <>''"
    //mId,date,category,goods_sid=>1
    val ucgRawRdd = hiveContext.sql(sql).rdd.map(row => ((row.getString(0), row.getString(1).substring(0, 10), row.getString(3),
      row.getString(4)), 1))
    //category,goods,date=>memId,pv
    val ucAcuRdd = ucgRawRdd.reduceByKey(_ + _).map(x => ((x._1._3, x._1._4, x._1._2), (x._1._1, x._2)))
    val goodsSQL = "select category_sid, goods_sid,date,pv from recommendation.user_preference_goods_visit"
    val cgRdd = hiveContext.sql(goodsSQL).rdd.map(row => ((row.getString(0), row.getString(1), row.getString(2)), row.getLong(3)))
    val unormRdd = ucAcuRdd.join(cgRdd)
    val prefRdd = unormRdd.map(x=>{
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val today = sdf.format(new Date)
        val b=sdf.parse(today).getTime-sdf.parse(x._1._3).getTime
        val num=b/(1000*3600*24)
        //(goods_sid,mId)
        ((x._1._1,x._1._2,x._2._1._1),((x._2._1._2.toDouble/x._2._2)*scala.math.pow(0.85,num)))})
        .reduceByKey( _  + _).filter(_._2 > 0.0001)
        .map(u=>{
          ((u._1._3,u._1._1),Seq((u._1._2,u._2)))
        }).reduceByKey(_ ++ _)
        .mapValues(x=>{
          x.sortWith((a,b)=>a._2>b._2)
        })
      .flatMap(x=>{
        val mId=x._1._1
         x._2.take(30).map(y=>(mId,(y._1,y._2)))
      })


    //购买行为
    val shopsql = "select member_id, event_date, behavior_type, category_sid, goods_sid from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start and behavior_type=4000 and member_id is not null and  member_id <>'null' and member_id <>'NULL' and member_id <>''"
    //memberId,date,category,goods_sid=>1
    val shopRdd = hiveContext.sql(shopsql).rdd.map(row => (row.getString(0), row.getString(1).substring(0, 10), row.getString(3),
      row.getString(4), 2))
    val transShopRdd = shopRdd.map{case(memberId,date,category,goods_sid,degree)=>
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val today = sdf.format(new Date)
      val b=sdf.parse(today).getTime-sdf.parse(date).getTime
      val num=b/(1000*3600*24)
      (memberId,(goods_sid,degree *scala.math.pow(0.85,num)))
    }.filter(_._1.toLong>100000000000000L)
    val userRdd =  prefRdd.union(transShopRdd).map{case(memberId,(goods_sid,degree))=>
      ((memberId,goods_sid),degree)
    }.reduceByKey(_ + _).map { case ((memberId, goods_sid), degree) =>
      ((memberId.toLong-100000000000000L).toInt,(goods_sid,degree))
    }.filter(_._2._1.toLong < Int.MaxValue)

//    val idTransRdd = userRdd.map { case (uId, (pId, rate)) => uId }.distinct().zipWithUniqueId()
//    val jRdd = userRdd.join(idTransRdd)
//    val inputRdd = jRdd.map { case ((mId, ((pId, rate), uId))) => (uId, pId, rate) }
//    val umRdd = jRdd.map { case ((mId, ((pId, rate), uId))) => (uId, mId) }.distinct()

    val trainRDD = userRdd.map { case (userIndex, (productIndex, rating)) =>
      Rating(userIndex, productIndex.toInt, rating.toDouble)
    }.cache
    val rank = 10
    val iterator = 20
    val model = ALS.train(trainRDD, rank, iterator, 0.01)
    val upfRdd = model.userFeatures.cartesian(model.productFeatures)
    val upsRdd = upfRdd.map(up=>{
      val uId = up._1._1.toLong
      val pId = up._2._1
      val uf  = new DoubleMatrix(up._1._2)
      val pf  =  new DoubleMatrix(up._2._2)
      (uId,(pId,uf.dot(pf)))
    })

    val upsTopK = upsRdd.mapValues(v=>Seq(v)).reduceByKey(_ ++ _)
      .mapValues(x=>{
      x.sortWith((a,b)=>a._2>b._2)
    }).mapValues(x=>x.take(80))


    val mrpRdd = upsTopK.map(
    x=>{
      val mId=x._1+100000000000000L
      val ps = x._2
      val pids =ps.map(x=>x._1)
      (mId,pids.mkString("#"))
      }
    )
    mrpRdd.foreachPartition(partition => {
      try {
        val jedisCluster = RedisClient.jedisCluster
        partition.foreach(data => {
          jedisCluster.set("rcmd_als_"+data._1,data._2)
        })
      } catch {
        case e: Exception => e.printStackTrace()
      }
    })
    sc.stop()
  }

}

object LFM {
  def main(args: Array[String]) {
    val day = args(0).toInt
    val lrg = new LFM
    lrg.userRating(day)
  }
}
