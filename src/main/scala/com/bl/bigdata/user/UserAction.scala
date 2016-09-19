package com.bl.bigdata.user

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.util.{RedisClient, SparkFactory}
import org.apache.spark.sql.hive.HiveContext
import redis.clients.jedis.Jedis

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
    val prefRdd = unormRdd.map(x=>{
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val today = sdf.format(new Date)
      val b=sdf.parse(today).getTime-sdf.parse(x._1._2).getTime
      val num=b/(1000*3600*24)
      //(category_sid,mId)
      ((x._1._1,x._2._1._1),((x._2._1._2.toDouble/x._2._2)*scala.math.pow(0.75,num)))})
    .reduceByKey( _  + _).filter(_._2 > 0.00001)
    .map(u=>
    {
      (u._1._2,(u._1._1,u._2))
    })


    //购买行为
    val shopsql = "select member_id, event_date, behavior_type, category_sid, goods_sid from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start and behavior_type=4000 and member_id is not null and  member_id <>'null' and member_id <>'NULL' and member_id <>''"
    //memberId,date,category,goods_sid=>1
    //购买周期
    //category_sid,member_id,date
    val cbcRawRdd  = hiveContext.sql(shopsql).rdd.map(row=>((row.getString(3),row.getString(0)),(row.getString(1).substring(0,10))))
    val udcRdd = cbcRawRdd.mapValues(x=>Seq(x)).reduceByKey( _ ++ _).mapValues(x=>x.distinct.sorted).filter(_._2.size>1)
    val ccsRdd = udcRdd.mapValues(x=>
    {
      val ax = x.toArray
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val cycle = for(i<-0 until  ax.length-1) yield (sdf.parse(ax(i+1)).getTime-sdf.parse(ax(i)).getTime)/(1000*3600*24)
      cycle
    }).map(x=>{(x._1._1,x._2)}).reduceByKey(_ ++ _)

    val cmedRdd = ccsRdd.mapValues(x=>{
      val sx = x.sorted
      val count = sx.length
      val median: Double = if (count % 2 == 0) {
        val l = count / 2 - 1
        val r = l + 1
        (sx(l) + sx(r)).toDouble / 2
      } else sx(count / 2).toInt
      median
    })
    //category_sid,member_id,date
    val shopRdd = cbcRawRdd.mapValues(x=>Seq(x)).reduceByKey( _ ++ _).mapValues(x=>x.distinct.sorted.last)

    val shopCycRdd = shopRdd.map{case((category,memberId),date)=>(category,(memberId,date))}.leftOuterJoin(cmedRdd)
    .map{case(category,((memberId,date),median))=>
    {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val today = sdf.format(new Date)
      val b=sdf.parse(today).getTime-sdf.parse(date).getTime
      val num=b/(1000*3600*24)
      var weight:Double = 0
      if(median.isEmpty)
        {
          weight = 1
        }
      else
      {
        weight = 1/(1+Math.exp(Math.abs(median.get - num.toInt).toDouble))
      }

      (memberId,(category, (2*weight)))
    }}

    val userRdd =  prefRdd.union(shopCycRdd).map{case(memberId,(category,degree))=>
      ((memberId,category),degree)
    }.reduceByKey(_ + _).map { case ((memberId, category), degree) =>
      (memberId,(category,degree))
    }.mapValues(v=>Seq(v)).reduceByKey(_ ++ _)
  .mapValues(x=>{
                x.sortWith((a,b)=>a._2>b._2)
              }).mapValues(lt=>lt.map{case(x,y)=>x+":"+y}.mkString("#"))

    userRdd.foreachPartition(partition => {
      val jedis = RedisClient.jedisCluster
      partition.foreach(s => {
        println("cate_pref_"+s._1,s._2)
       jedis.set("cate_pref_"+s._1,s._2)
      })
     // jedis.close()
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
    //mId,date,category,goods_sid=>1
    val ucgRawRdd  = hiveContext.sql(sql).rdd.map(row=>((row.getString(0),row.getString(1).substring(0,10),row.getString(3),
      row.getString(4)),1))
    //cateId,goods_sid,date=>memId,pv
    val ucAcuRdd = ucgRawRdd.reduceByKey(_ + _).map{case((mId,date,cate,gId),deg)=> ((cate,gId,date),(mId,deg))}
    val goodsSQL ="select category_sid, goods_sid,date,pv from recommendation.user_preference_goods_visit"
    //cateId,goods,date,count pv
    val cgRdd  = hiveContext.sql(goodsSQL).rdd.map(row=>((row.getString(0),row.getString(1),row.getString(2)),row.getLong(3)))
    val unormRdd = ucAcuRdd.join(cgRdd)
    //((103466,(21021460629373192854469,4)),0.18402591023557582,33,2016-04-14)
    val prefRdd =  unormRdd.map{
      case(((cate,gId,date),((mId,deg),pv)))=>
      {
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val today = sdf.format(new Date)
        val b=sdf.parse(today).getTime-sdf.parse(date).getTime
        val num=b/(1000*3600*24)
        ((mId,cate,gId),(deg.toDouble/pv.toDouble)*scala.math.pow(0.75,num))
      }
    }.reduceByKey( _  + _).filter(_._2 > 0.000001)
      .map{case ((mId,cate,gId),deg)=>
        (mId,(cate,gId,deg))
      }


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
      (memberId,(category,goods_sid,degree *scala.math.pow(0.9,num)))
    }

    val userRdd =  prefRdd.union(transShopRdd).map{case(memberId,(category,goods_sid,degree))=>
      ((memberId,category,goods_sid),degree)
    }.reduceByKey(_ + _)
      .map { case ((memberId,category, goods_sid), degree) => ((memberId,category),Seq((goods_sid,degree.toDouble)))}
      .reduceByKey(_ ++ _)
      .mapValues(x=>{
        x.sortWith((a,b)=>a._2>b._2)
              })
      .map(x=>{
              val mId=x._1._1
              val cateId = x._1._2
              val goods = x._2.take(5).map{case(m,n)=>m}.mkString(",")
              (mId,Seq(cateId+":"+goods))
            }).reduceByKey(_ ++ _).mapValues(cgs=>cgs.mkString("#"))

    userRdd.foreachPartition(partition => {
      val jedisCluster = RedisClient.jedisCluster
      partition.foreach(s => {
        jedisCluster.set("rcmd_goods_pref_"+s._1,s._2)
      })
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
