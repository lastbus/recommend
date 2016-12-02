package com.bl.bigdata.bean

import java.util.List

import com.infomatiq.jsi.Point

/**
  * Created by MK33 on 2016/10/12.
  */
class Polygon {

  private var corners: List[Point] = null

  def this(corners: List[Point]) {
    this()
    this.corners = corners
  }

  def getCorners(): List[Point] = {
    return corners
  }

  def setCorners(corners: List[Point]) {
    this.corners = corners
  }

}
