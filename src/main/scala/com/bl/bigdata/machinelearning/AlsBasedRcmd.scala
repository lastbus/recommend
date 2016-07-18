package com.bl.bigdata.machinelearning

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.util.{RedisClient, SparkFactory}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/6/8.
 */
class AlsBasedRcmd {

  def userRating(): Unit = {
    val sc = SparkFactory.getSparkContext("ALS_Data")
    val hiveContext = new HiveContext(sc)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * 60))
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
      ((x._1._1,x._1._2,x._2._1._1),((x._2._1._2.toDouble/x._2._2)*scala.math.pow(0.75,num)))})
      .reduceByKey( _  + _).filter(_._2 > 0.001)
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
      // x._2.take(5).map(y=>(mId,(y._1,y._2)))
      //由5个改为20个
      x._2.take(20).map(y=>(mId,(y._1,y._2)))

    })


    //购买行为
    val shopsql = "select member_id, event_date, behavior_type, category_sid, goods_sid from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start and behavior_type=4000 and member_id is not null and  member_id <>'null' and member_id <>'NULL' and member_id <>''"
    //memberId,date,category,goods_sid=>1
    val shopRdd = hiveContext.sql(sql).rdd.map(row => (row.getString(0), row.getString(1).substring(0, 10), row.getString(3),
      row.getString(4), 2))
    val transShopRdd = shopRdd.map{case(memberId,date,category,goods_sid,degree)=>
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val today = sdf.format(new Date)
      val b=sdf.parse(today).getTime-sdf.parse(date).getTime
      val num=b/(1000*3600*24)
      (memberId,(goods_sid,degree *scala.math.pow(0.75,num)))
    }
   val userRdd =  prefRdd.union(transShopRdd).map{case(memberId,(goods_sid,degree))=>
     ((memberId,goods_sid),degree)
   }.reduceByKey(_ + _).map { case ((memberId, goods_sid), degree) =>
     (memberId,(goods_sid,degree))
   }

    val idTransRdd = userRdd.map { case (uId, (pId, rate)) => uId }.distinct().zipWithUniqueId()
    val jRdd = userRdd.join(idTransRdd)
    val inputRdd = jRdd.map { case ((mId, ((pId, rate), uId))) => (uId, pId, rate) }
    val users = jRdd.map { case ((mId, ((pId, rate), uId))) => (uId, mId) }.distinct().collect
    val trainRDD = inputRdd.map { case (userIndex, productIndex, rating) =>
      Rating(userIndex.toInt, productIndex.toInt, rating.toDouble)
    }.cache
    val rank = 10
    val iterator = 20
    val model = ALS.train(trainRDD, rank, iterator, 0.01)
    //val bmodel = sc.broadcast(model)
    //val dmodel = bmodel.value
    //val jedis = RedisClient.pool.getResource
    val jedisCluster = RedisClient.jedisCluster
    var cnt = 1;
    val upRdd = users.foreach { case (uId, mId) => {
      cnt+=1
      val K = 60
      val topKRecs = model.recommendProducts(uId.toInt, K)
      val prods = topKRecs.map(r => {
        r.product
      }
      ).mkString("#")
      println(mId+":"+prods)
      println(cnt+"-------------------------")
      jedisCluster.set("rcmd_als_" + mId, prods)
    }
    }
    //jedisCluster.close()
    sc.stop()
  }


}

object AlsBasedRcmd {
  def main(args: Array[String]) {
    val lrg = new AlsBasedRcmd
    lrg.userRating()
  }
}
