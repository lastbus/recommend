package com.bl.bigdata.service

import java.util.{ArrayList, HashMap, List}

import com.bl.bigdata.bean.Polygon
import com.infomatiq.jsi.{Point, Rectangle}

/**
  * Created by MK33 on 2016/10/12.
  */
class GeoFenceServiceImpl extends GeoFenceService {

  val mbrService = new MBRServiceImpl
  val rtreeService = new RtreeServiceImpl
  val pipService = new PointInPolygonServiceImpl

  @throws[Exception]
  def addPolygon(id: Integer, polygon: Polygon) {
    if (polygon == null || id == null) {
      throw new Exception("argument is null")
    }
    if (polygon.getCorners == null || polygon.getCorners.size < 3) {
      throw new Exception("polygon is illegal")
    }
    val rectangle: Rectangle = mbrService.getMBROfPolygon(polygon)
    rtreeService.addRectangleIntoRtree(rectangle, id)
  }

  @throws[Exception]
  def getMBRList(point: Point): List[Integer] = {
    val ids: List[Integer] = rtreeService.getRectangleListByPoint(point)
    return ids
  }

  @throws[Exception]
  def getPolygonList(point: Point, polygonMap: HashMap[Integer, Polygon]): List[Integer] = {
    val ids: List[Integer] = new ArrayList[Integer]
    if (polygonMap != null && !polygonMap.isEmpty) {
      import scala.collection.JavaConversions._
      for (id <- polygonMap.keySet) {
        if (pipService.pointInPolygon(point, polygonMap.get(id))) {
          ids.add(id)
        }
      }
    }
    return ids
  }
}
