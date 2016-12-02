package com.bl.bigdata.service

import java.util.{HashMap, List}

import com.bl.bigdata.bean.Polygon
import com.infomatiq.jsi.Point

/**
  * Created by MK33 on 2016/10/12.
  */
trait GeoFenceService extends Serializable{
  @throws[Exception]
  def addPolygon(id: Integer, polygon: Polygon)

  @throws[Exception]
  def getMBRList(point: Point): List[Integer]

  @throws[Exception]
  def getPolygonList(point: Point, polygonMap: HashMap[Integer, Polygon]): List[Integer]
}
