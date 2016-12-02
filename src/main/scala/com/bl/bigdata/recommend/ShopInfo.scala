package com.bl.bigdata.recommend

import java.sql.DriverManager
import java.util
import java.util.{ArrayList, HashMap, List}

import com.bl.bigdata.bean.Polygon
import com.bl.bigdata.service.GeoFenceServiceImpl
import com.infomatiq.jsi.Point
import org.json.JSONObject

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * 根据门店的配送范围经纬度，然后判断一个经纬度属于哪个门店的配送区域
  * Created by MK33 on 2016/10/12.
  */
class ShopInfo(shopList: Seq[(String, String, String, String)]) extends Serializable {

  val polygonMap = new HashMap[Integer, Polygon]()
  val indexToStoreMap = mutable.HashMap[Integer, (String, String, String)]()
  val geoFenceTool = new GeoFenceServiceImpl

//  val shopList = GetShopList.getStoreList()
  var index = 0
  // 得到门店负责区域的多边形
  for (store <- shopList) {
    //      println(store._1)
    val json = new JSONObject(store._4)
    val scopeVOList = json.getJSONArray("scopeVOList")
    // 一个店可以负责多个区域
    for (i <- 0 until scopeVOList.length()) {
      index += 1
      val lngLatList = scopeVOList.getJSONObject(i).getJSONArray("lngLatList")
      val corners = new ArrayList[Point]()
      for (j <- 0 until lngLatList.length()) {
        corners.add(new Point(lngLatList.getJSONArray(j).getDouble(0).asInstanceOf[Float],
          lngLatList.getJSONArray(j).getDouble(1).asInstanceOf[Float]))
      }
      val polygon = new Polygon(corners)
      polygonMap(index) = polygon
      indexToStoreMap(index) = (store._1, store._2, store._3)
    }
  }

  for ((index, polygon) <- polygonMap)
    geoFenceTool.addPolygon(index, polygon)

//  val curPoint = new Point(121.490218f, 31.263580f)
//  val result = getStores(curPoint)
//  result.foreach(println)


  def getStores(point: Point): List[(String, String, String)] = {
    val tempIds = geoFenceTool.getMBRList(point)
    val matchMap = new util.HashMap[Integer, Polygon]()
    for (i <- tempIds) matchMap(i) = polygonMap(i)
    val result = geoFenceTool.getPolygonList(point, matchMap)
    for (i <- result) yield indexToStoreMap(i)
  }

  def getStores(longitude: Float, latitude: Float): List[(String, String, String)] = {
    getStores(new Point(longitude, latitude))
  }


  /**
    * get stores information from database
    *
    * @return List((storeName, storeLocation, storeLngLat, storeScope))
    */
  private def getStoreList(): scala.collection.immutable.List[(String, String, String, String)] = {
    Class.forName("oracle.jdbc.driver.OracleDriver")

    val url = "jdbc:oracle:thin:@10.201.48.18:1521:report"
    val username = "idmdata"
    val password = "bigdata915"
    val conn = DriverManager.getConnection(url, username, password)
    val stmt = conn.createStatement()
    var list: scala.collection.immutable.List[(String, String, String, String)] = Nil
    val result = stmt.executeQuery(" select store_name, store_location, store_lnglat, store_scope  from idmdata.dim_site_scope ")
    while (result.next()) {
      val storeName = result.getString(1)
      val storeLocation = result.getString(2)
      val storeLngLat = result.getString(3)
      val storeScope = result.getString(4)
      list = (storeName, storeLocation, storeLngLat, storeScope) :: list
    }
    list
  }

}
