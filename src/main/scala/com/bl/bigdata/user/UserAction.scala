package com.bl.bigdata.user

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.util.{RedisClient, SparkFactory}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/5/17.
 */
class UserAction {


  def categoryPref(): Unit = {
    val sc = SparkFactory.getSparkContext("category_pref")
    val hiveContext = new HiveContext(sc)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * 60))
    val sql = "select member_id, event_date, behavior_type, category_sid, goods_sid from recommendation.user_behavior_raw_data  " +
    s"where dt >= $start and behavior_type=1000"
    //ck,date,category,=>1
    val ucRawRdd  = hiveContext.sql(sql).rdd.map(row=>((row.getString(0),row.getString(1).substring(0,10),row.getString(3)),1))
    //category,date=>ck,pv
    val ucAcuRdd = ucRawRdd.reduceByKey(_ + _).map(x=> ((x._1._3,x._1._2),(x._1._1,x._2)))
    //val ucRawRdd  = hiveContext.sql(sql).rdd.map(row=>((row.getString(3),row.getString(1).substring(0,10)),row.getString(0)))
    val catesSql ="select category_sid,date,pv from recommendation.user_preference_category_visit"
    //category,date,count pv
    val catesRdd  = hiveContext.sql(catesSql).rdd.map(row=>((row.getString(0), row.getString(1)),row.getLong(2)))
    //((102564,2016-04-16),((88867259525614535970958,1),3))
    val unormRdd = ucAcuRdd.join(catesRdd)
    //((103466,(21021460629373192854469,4)),0.18402591023557582,33,2016-04-14)
    val prefRdd = unormRdd.map(x=>{
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val today = sdf.format(new Date)
      val b=sdf.parse(today).getTime-sdf.parse(x._1._2).getTime
      val num=b/(1000*3600*24)
      //(goods_sid,mId)
      ((x._1._1,x._2._1._1),((x._2._1._2.toDouble/x._2._2)*scala.math.pow(0.75,num)))})
    .reduceByKey( _  + _).filter(_._2 > 0.001)
      .map(u=>{
      (u._1._2,Seq((u._1._1,u._2)))
    }).reduceByKey(_ ++ _)
      .mapValues(x=>{
            x.sortWith((a,b)=>a._2>b._2)
          }).mapValues(lt=>lt.map{case(x,y)=>x+":"+y}.mkString("#"))

    prefRdd.foreachPartition(partition => {
      val jedis = RedisClient.pool.getResource
      partition.foreach(s => {
       jedis.set("cate_pref_"+s._1,s._2)
      })
      jedis.close()
    })
}

  /**
   *
   */
  def goodsPref(): Unit = {
    val sc = SparkFactory.getSparkContext("user_goods_pref")
    val hiveContext = new HiveContext(sc)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * 60))
    val sql = "select member_id, event_date, behavior_type, category_sid, goods_sid from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start and behavior_type=1000"
    //ck,date,category,goods_sid=>1
    val ucgRawRdd  = hiveContext.sql(sql).rdd.map(row=>((row.getString(0),row.getString(1).substring(0,10),row.getString(3),
      row.getString(4)),1))
    //category,goods,date=>memId,pv
    val ucAcuRdd = ucgRawRdd.reduceByKey(_ + _).map(x=> ((x._1._3,x._1._4,x._1._2),(x._1._1,x._2)))
    //val ucRawRdd  = hiveContext.sql(sql).rdd.map(row=>((row.getString(3),row.getString(1).substring(0,10)),row.getString(0)))
    val goodsSQL ="select category_sid, goods_sid,date,pv from recommendation.user_preference_goods_visit"
    //category,goods,date,count pv
    val cgRdd  = hiveContext.sql(goodsSQL).rdd.map(row=>((row.getString(0), row.getString(1),row.getString(2)),row.getLong(3)))
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
    .map(x=>{
      val mId=x._1._1
      val cateId = x._1._2
      val goods = x._2.take(5).map{case(m,n)=>m}.mkString(",")
      (mId,Seq(cateId+":"+goods))
    }).reduceByKey(_ ++ _).mapValues(cgs=>cgs.mkString("#"))

    prefRdd.foreachPartition(partition => {
      val jedis = RedisClient.pool.getResource
      partition.foreach(s => {
        jedis.set("rcmd_goods_pref_"+s._1,s._2)
      })
      jedis.close()
    })

    sc.stop()
  }
  def dateDiff (data1:String,date2:String):Double={
    val sdf =new SimpleDateFormat("yyyy-mm-dd")
    val b=sdf.parse(data1).getTime-sdf.parse(date2).getTime
    val num=b/(1000*3600*24)
    num
  }

  /**
   * 被goodsPref取代
   */
  def userViewHistory():Unit={
    val sc = SparkFactory.getSparkContext("最近两个月浏览未购买商品 按时间排序")
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * 60))
    val sql = "select member_id, event_date, behavior_type, category_sid, goods_sid " +
      " from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start"

    val a = ReadData.readHive(sc, sql).map { case Array(cookie, date, behavior, category, goods) =>
      ((cookie, date.substring(0, date.indexOf(" "))), behavior, (category, goods)) }
    val browserRDD = a filter { case ((cookie, date), behaviorID, goodsID) => behaviorID.equals("1000")}
    val buyRDD = a filter { case ((cookie, date), behaviorID, goodsID) => behaviorID.equals("4000")}
    val browserNotBuy = browserRDD subtract buyRDD map (s => (s._1._1, Seq((s._3._1, (s._3._2, s._1._2))))) reduceByKey ((s1,s2) => s1 ++ s2) map (item => {
      ("rcmd_memberid_view_" + item._1, item._2.groupBy(_._1).map(s => s._1 + ":" + s._2.map(_._2).sortWith(_._2 > _._2).map(_._1).distinct.mkString(",")).mkString("#"))})
    browserNotBuy.foreachPartition(partition => {
      val jedis = RedisClient.pool.getResource
      partition.foreach(s => {
        jedis.set(s._1,s._2)
      })
      jedis.close()
    })
    sc.stop()
  }

}


object UserAction {
  def main(args: Array[String]) {
    val cp  = new UserAction
    cp.categoryPref()
    cp.goodsPref()
  }

}
