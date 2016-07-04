package com.bl.bigdata.user.profile

import java.text.SimpleDateFormat
import java.util.Date

import com.bl.bigdata.config.HbaseConfig
import com.bl.bigdata.datasource.ReadData
import com.bl.bigdata.util.SparkFactory
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/6/1.
 */
class CategoryPreference {

    def categoryPref(): Unit = {
    val sc = SparkFactory.getSparkContext("user_profile_category_preference")
    val hiveContext = new HiveContext(sc)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * 60))
    val sql = "select member_id, event_date, behavior_type, category_sid, goods_sid from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start and behavior_type=1000 and member_id is not null and member_id <> 'NULL'"
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
      ((x._1._1,x._2._1._1),((x._2._1._2.toDouble/x._2._2)*scala.math.pow(0.95,num)))})
      .reduceByKey( _  + _).map(u=>{
      (u._1._2,Seq((u._1._1,u._2)))
    }).reduceByKey(_ ++ _).mapValues(x=>{
      x.sortWith((a,b)=>a._2>b._2)
    }).mapValues(lt=>lt.map{case(x,y)=>x+":"+y}.mkString("#"))

      prefRdd.foreachPartition{
        x=> {
          val conf = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.property.clientPort", HbaseConfig.hbase_zookeeper_property_clientPort)
          conf.set("hbase.zookeeper.quorum",HbaseConfig.hbase_zookeeper_quorum )
          conf.set("hbase.master", HbaseConfig.hbase_master)
          val cookieTbl = new HTable(conf, TableName.valueOf(HbaseConfig.user_profile))
          cookieTbl.setAutoFlush(false, false)
          cookieTbl.setWriteBufferSize(3*1024*1024)
          x.foreach { y => {
            if(!y._1.isEmpty) {
              val p = new Put(Bytes.toBytes(y._1.toString))
              if (y._2.toString.isEmpty != null && !y._2.toString.isEmpty) {
                p.addColumn(HbaseConfig.user_profile_interests_family.getBytes, HbaseConfig.user_profile_interests_category_qualifer.getBytes,
                  Bytes.toBytes(y._2.toString))
                cookieTbl.put(p)
              }
            }
          }
          }
          cookieTbl.flushCommits()
        }
      }


  }
  def dateDiff (data1:String,date2:String):Double={
    val sdf =new SimpleDateFormat("yyyy-mm-dd")
    val b=sdf.parse(data1).getTime-sdf.parse(date2).getTime
    val num=b/(1000*3600*24)
    num
  }

  def userShopHistory():Unit={
    val sc = SparkFactory.getSparkContext("最近两个月购买商品 按时间排序")
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date0 = new Date
    val start = sdf.format(new Date(date0.getTime - 24000L * 3600 * 60))
    val sql = "select member_id, event_date, behavior_type, category_sid, goods_sid " +
      " from recommendation.user_behavior_raw_data  " +
      s"where dt >= $start"
    val a = ReadData.readHive(sc, sql).map { case Array(cookie, date, behavior, category, goods) =>
      ((cookie, date.substring(0, date.indexOf(" "))), behavior, (category, goods)) }
    val buyRDD = a filter { case ((cookie, date), behaviorID, goodsID) => behaviorID.equals("4000")}
    val orderdRdd = buyRDD.map (s => (s._1._1, Seq((s._3._1, (s._3._2, s._1._2))))) reduceByKey ((s1,s2) => s1 ++ s2) map (item => {
      (item._1, item._2.groupBy(_._1).map(s => s._1 + ":" + s._2.map(_._2).sortWith(_._2 > _._2).map(_._1).distinct.mkString(",")).mkString("#"))})

    orderdRdd.foreachPartition{
      x=> {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.property.clientPort", HbaseConfig.hbase_zookeeper_property_clientPort)
        conf.set("hbase.zookeeper.quorum",HbaseConfig.hbase_zookeeper_quorum )
        conf.set("hbase.master", HbaseConfig.hbase_master)
        val cookieTbl = new HTable(conf, TableName.valueOf(HbaseConfig.user_profile))
        cookieTbl.setAutoFlush(false, false)
        cookieTbl.setWriteBufferSize(3*1024*1024)
        x.foreach { y => {
          if (!y._1.isEmpty) {
            val p = new Put(Bytes.toBytes(y._1.toString))
            p.addColumn(HbaseConfig.user_profile_behavior_characteristics_family.getBytes,
              HbaseConfig.user_profile_behavior_characteristics_last_two_month_shoped_qualifier.getBytes,
              Bytes.toBytes(y._2.toString))
            cookieTbl.put(p)
          }
        }
        }
        cookieTbl.flushCommits()
      }
    }

    sc.stop()
  }
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
      (item._1, item._2.groupBy(_._1).map(s => s._1 + ":" + s._2.map(_._2).sortWith(_._2 > _._2).map(_._1).distinct.mkString(",")).mkString("#"))})
    browserNotBuy.foreachPartition{
      x=> {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.property.clientPort", HbaseConfig.hbase_zookeeper_property_clientPort)
        conf.set("hbase.zookeeper.quorum",HbaseConfig.hbase_zookeeper_quorum )
        conf.set("hbase.master", HbaseConfig.hbase_master)
        val cookieTbl = new HTable(conf, TableName.valueOf(HbaseConfig.user_profile))
        cookieTbl.setAutoFlush(false, false)
        cookieTbl.setWriteBufferSize(3*1024*1024)
        x.foreach { y => {
          if (!y._1.isEmpty) {
            val p = new Put(Bytes.toBytes(y._1.toString))
            p.addColumn(HbaseConfig.user_profile_behavior_characteristics_family.getBytes,
              HbaseConfig.user_profile_behavior_characteristics_last_two_month_browsed_qualifier.getBytes,
              Bytes.toBytes(y._2.toString))
            cookieTbl.put(p)
          }
        }
        }
        cookieTbl.flushCommits()
      }
    }

    //sc.stop()
  }

}


object CategoryPreference {
  def main(args: Array[String]) {
    val cp = new CategoryPreference
    cp.categoryPref()
    cp.userViewHistory()
    cp.userShopHistory()
  }
}
