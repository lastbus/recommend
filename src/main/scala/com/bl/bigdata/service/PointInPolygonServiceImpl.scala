package com.bl.bigdata.service

import com.bl.bigdata.bean.Polygon
import com.infomatiq.jsi.Point

class PointInPolygonServiceImpl extends PointInPolygonService {

  def pointInPolygon(point: Point, polygon: Polygon): Boolean = {
    if (polygon == null || point == null) {
      throw new Exception("argument is null")
    }
    if (polygon.getCorners == null || polygon.getCorners.size < 3) {
      throw new Exception("polygon is illegal")
    }
    var oddNodes = false
    val polyCorners = polygon.getCorners
    val polyCornersSize = polyCorners.size
    var i = 0
    var j = polyCornersSize - 1

    import scala.util.control.Breaks._

    breakable {
      while (i < polyCornersSize) {
        if (isPointOnSegment(point, polyCorners.get(i), polyCorners.get(j))) {
          oddNodes = true
          break
        }
        j = i
        i += 1
      }
    }

    if (oddNodes) {
      return oddNodes
    }

    /**
      * judge point is in polygon
      */
    i = polyCornersSize - 1
    j = polyCornersSize - 1
    i = 0
    while (i < polyCornersSize) {
      if ((polyCorners.get(i).y < point.y && polyCorners.get(j).y >= point.y || polyCorners.get(j).y < point.y && polyCorners.get(i).y >= point.y) &&
        (polyCorners.get(i).x <= point.x || polyCorners.get(j).x <= point.x))
      {
        oddNodes ^= (polyCorners.get(i).x +
          (point.y - polyCorners.get(i).y) / (polyCorners.get(j).y - polyCorners.get(i).y) * (polyCorners.get(j).x - polyCorners.get(i).x) < point.x)
      }
      j = i
      i += 1
    }
    return oddNodes
  }

  private def isPointOnSegment(point: Point, segmentPoint1: Point, segmentPoint2: Point): Boolean = {
    if (onSegment(point, segmentPoint1, segmentPoint2) && direction(point, segmentPoint1, segmentPoint2) == 0) {
      return true
    }
    else {
      return false
    }
  }

  private def direction(point: Point, segmentPoint1: Point, segmentPoint2: Point): Float = {
    return (segmentPoint1.x - point.x) * (segmentPoint2.y - point.y) - (segmentPoint2.x - point.x) * (segmentPoint1.y - point.y)
  }

  private def onSegment(point: Point, segmentPoint1: Point, segmentPoint2: Point): Boolean = {
    val maxX: Float = if (segmentPoint1.x > segmentPoint2.x) segmentPoint1.x
    else segmentPoint2.x
    val minX: Float = if (segmentPoint1.x < segmentPoint2.x) segmentPoint1.x
    else segmentPoint2.x
    val maxY: Float = if (segmentPoint1.y > segmentPoint2.y) segmentPoint1.y
    else segmentPoint2.y
    val minY: Float = if (segmentPoint1.y < segmentPoint2.y) segmentPoint1.y
    else segmentPoint2.y
    if (point.x >= minX && point.x <= maxX && point.y >= minY && point.y <= maxY) {
      return true
    }
    else {
      return false
    }
  }
}
