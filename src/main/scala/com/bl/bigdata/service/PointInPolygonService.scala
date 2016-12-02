package com.bl.bigdata.service

import com.bl.bigdata.bean.Polygon
import com.infomatiq.jsi.Point;

trait PointInPolygonService extends Serializable {
	/**
	 * check whether point in polygon including slides
	 * @param point
	 * @param polygon
	 * @return
	 */
	def pointInPolygon(point: Point, polygon: Polygon): Boolean

}
