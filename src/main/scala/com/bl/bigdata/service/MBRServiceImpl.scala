package com.bl.bigdata.service

import com.bl.bigdata.bean.Polygon
import com.infomatiq.jsi.Rectangle;

class MBRServiceImpl extends MBRService {

	def getMBROfPolygon(polygon: Polygon): Rectangle = {
		if(polygon == null){
			throw new Exception("argument is null");
		}
		if(polygon.getCorners() == null || polygon.getCorners().size() < 3){
			throw new Exception("polygon is illegal");
		}
		val polygonCorners = polygon.getCorners()
		var minX = polygonCorners.get(0).x
		var minY = polygonCorners.get(0).y
		var maxX = polygonCorners.get(0).x
		var maxY = polygonCorners.get(0).y
		for(i <- 0 until polygonCorners.size() ){
			if(polygonCorners.get(i).x > maxX){
				maxX = polygonCorners.get(i).x
			}
			if(polygonCorners.get(i).x < minX){
				minX = polygonCorners.get(i).x
			}
			if(polygonCorners.get(i).y > maxY){
				maxY = polygonCorners.get(i).y
			}
			if(polygonCorners.get(i).y < minY){
				minY = polygonCorners.get(i).y
			}
		}
		return new Rectangle(minX,minY,maxX,maxY)
	}

}
