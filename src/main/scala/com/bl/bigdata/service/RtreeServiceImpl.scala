package com.bl.bigdata.service

import java.util.{ArrayList, List}

import com.infomatiq.jsi.{Point, Rectangle, SpatialIndex}
import com.infomatiq.jsi.rtree.RTree
import gnu.trove.TIntProcedure

class RtreeServiceImpl extends RtreeService {
  private var si: SpatialIndex = null

  private def getCurrentRtreeFromCache: SpatialIndex = {
    if (si == null) {
      si = new RTree
      si.init(null)
    }
    return si
  }

  @throws[Exception]
  def addRectangleIntoRtree(rectangle: Rectangle, id: Integer) {
    if (rectangle == null || id == null) {
      throw new Exception("argument is null")
    }
    val si: SpatialIndex = getCurrentRtreeFromCache
    if (si == null) {
      throw new Exception("rtree is null")
    }
    si.add(rectangle, id)
  }

  @throws[Exception]
  def getRectangleListByPoint(point: Point): List[Integer] = {
    if (point == null) {
      throw new Exception("argument is null")
    }
    val si: SpatialIndex = getCurrentRtreeFromCache
    if (si == null) {
      throw new Exception("rtree is null")
    }
    val myProc = new SaveToListProcedure
    si.nearest(point, myProc, 0f)
    return myProc.getIds()
  }

  class SaveToListProcedure extends TIntProcedure {
    val ids: List[Integer] = new ArrayList[Integer]

    def execute(id: Int): Boolean = {
      ids.add(id)
      return true
    }

    def getIds(): List[Integer] = {
      return ids
    }
  }

}
