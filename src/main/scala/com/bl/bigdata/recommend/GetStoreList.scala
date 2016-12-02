package com.bl.bigdata.recommend

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 从 oracle 数据库获取a线下门店 store 的列表，测试环境连接不了生产数据库，在本地把生产数据库导入到测试环境吧
  * Created by MK33 on 2016/10/12.
  */
object GetStoreList {
  private var shopList: List[(String, String, String, String)] = Nil


  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("test")
      .set("hive.metastore.warehouse.dir", "hdfs://m78sit:8020/user/hive/warehouse"))
    val storesList = getStoreList()
    val storeRDD = sc.parallelize(storesList).map(s => s._1 + "\t" + s._2 + "\t" + s._3 + "\t" + s._4)

    storeRDD.coalesce(1).saveAsTextFile("hdfs://m78sit:8020/tmp/store")




  }

  /**
    * get stores information from database
    *
    * @return List((storeName, storeLocation, storeLngLat, storeScope))
    */
  def getStoreList(): scala.collection.immutable.List[(String, String, String, String)] = {
    if (shopList == null || shopList.isEmpty) fetch()
    shopList
  }

  private def fetch(): Unit = {
    Class.forName("oracle.jdbc.driver.OracleDriver")

    val url = "jdbc:oracle:thin:@10.201.48.18:1521:report"
    val username = "idmdata"
    val password = "bigdata915"
    val conn = DriverManager.getConnection(url, username, password)
    val stmt = conn.createStatement()
    val result = stmt.executeQuery(" select store_name, store_location, store_lnglat, store_scope  from idmdata.dim_site_scope ")
    while (result.next()) {
      val storeName = result.getString(1)
      val storeLocation = result.getString(2)
      val storeLngLat = result.getString(3)
      val storeScope = result.getString(4)
      shopList = (storeName, storeLocation, storeLngLat, storeScope) :: shopList
    }
  }


}
