package com.bl.bigdata.service

import java.util.List

import com.infomatiq.jsi.{Point, Rectangle};

trait RtreeService extends Serializable {
	/**
	 * add Rectangle to r-tree and re-index
	 * @param rectangle
	 * @param id
	 * @return success or fail
	 */
	def addRectangleIntoRtree(rectangle: Rectangle, id: Integer)
	
	/**
	 * get Rectangle list that cover the point using r-tree 
	 * @param point
	 * @return
	 */
	def getRectangleListByPoint(point: Point): List[Integer]

}
