package com.bl.bigdata.machinelearning

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.util.SparkFactory
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/6/13.
 */
class UserCF {
  def usertaste(): Unit = {
    val sc = SparkFactory.getSparkContext("user_cf")
    val hiveContext = new HiveContext(sc)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * 30))
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
      ((x._1._1,x._1._2,x._2._1._1),((x._2._1._2.toDouble/x._2._2)*scala.math.pow(0.95,num)))})
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
      x._2.take(10).map(y=>(mId,(y._1,y._2)))

    })

    val idTransRdd = prefRdd.map { case (uId, (pId, rate)) => uId }.distinct().zipWithUniqueId()
    val jRdd = prefRdd.join(idTransRdd)
    val uprRdd = jRdd.map { case ((mId, ((pId, rate), uId))) => (uId, (pId, rate)) }
    val IdRdd = jRdd.map { case ((mId, ((pId, rate), uId))) => (uId, mId) }.distinct()
    val trainRDD = uprRdd.map { case (userIndex, (productIndex, rating)) =>
      (userIndex, Map(productIndex.toString -> rating.toDouble))
    }.reduceByKey(_ ++ _)

//    val usersRdd = trainRDD.cartesian(trainRDD)
//      .map {
//        case (ud1, ud2) =>
//          val id1 = ud1._1
//          val id2 = ud2._1
//          val d1 = ud1._2
//          val d2 = ud2._2
//          var sum = 0.0
//          var divider1 = 0.0
//          d1.foreach { case (k, v) => {
//            val tmp = d2.get(k)
//            if (!tmp.isEmpty) sum += (v * tmp.get)
//            divider1 += v * v
//          }
//          }
//
//          val divider2 = d2.map(s => s._2 * s._2).sum
//          (id1, (id2, sum / (scala.math.sqrt(divider1) * scala.math.sqrt(divider2))))
//      }.filter(s => s._2._2.toDouble > 0.0001 && s._1 != s._2._1).mapValues(v => Seq(v))
//      .reduceByKey(_ ++ _).mapValues(v => v.sortWith((a, b) => a._2 > b._2)).mapValues(v => v.take(10))
//    //
  val randIndexRdd = trainRDD.map(user=>{
      val nkey = scala.util.Random.nextInt(20)
      (nkey,user)
    })
    randIndexRdd.foreach(println)
   val partUserRdd = randIndexRdd.join(randIndexRdd).mapValues(u=>Seq(u)).reduceByKey( _ ++ _)
    .flatMapValues(us=>{us.union(us)
    })

    partUserRdd.foreach(println)
//     val tmpUpRdd = partUserRdd
//     .map(kv=>kv._2).map {
//              case (ud1, ud2) =>
//                val id1 = ud1._1
//                val id2 = ud2._1
//                val d1 = ud1._2
//                val d2 = ud2._2
//                var sum = 0.0
//                var divider1 = 0.0
//                d1.foreach { case (k, v) => {
//                  val tmp = d2.get(k)
//                  if (!tmp.isEmpty) sum += (v * tmp.get)
//                  divider1 += v * v
//                }
//                }
//                val divider2 = d2.map(s => s._2 * s._2).sum
//                (id1, (id2, sum / (scala.math.sqrt(divider1) * scala.math.sqrt(divider2))))
//            }.filter(s => s._2._2.toDouble > 0.0001 && s._1 != s._2._1).mapValues(v => Seq(v))
//            .reduceByKey(_ ++ _).mapValues(v => v.sortWith((a, b) => a._2 > b._2)).mapValues(v => v.take(10))
//
//    tmpUpRdd.saveAsTextFile("/home/hdfs/newUC")
//    val nbsRdd = tmpUpRdd.flatMap(x => {
//      val uId1 = x._1
//      val nbs = x._2
//      nbs.map(y => ((y._1, (y._2, uId1))))
//    }).join(uprRdd)
//      .map { case (nb, ((sim, uId), (pId, deg))) => {
//        (uId, (pId, (sim * deg)))
//      }
//      }.mapValues(v => Seq(v)).reduceByKey(_ ++ _).mapValues(v => v.sortWith((a, b) => a._2 > b._2).take(60)).join(IdRdd)
//      .map(v => {
//        val data = v._2
//        val pds = data._1
//        val spds = pds.map(x =>
//          x._1
//        ).distinct.mkString("#")
//        val mId = data._2
//        (mId, spds)
//      }
//
//      )
//    nbsRdd.foreachPartition(partition => {
//      try {
//        val jedis = RedisClient.pool.getResource
//        partition.foreach(data => {
//          println("rcmd_usercf_"+data._1)
//          jedis.set("rcmd_usercf_"+data._1, data._2)
//        })
//        jedis.close()
//      } catch {
//        case e: Exception => e.printStackTrace()
//      }
//
//    })
    sc.stop()
  }


}

object UserCF {
  def main(args: Array[String]) {
    val uf = new UserCF
    uf.usertaste()
  }
}
