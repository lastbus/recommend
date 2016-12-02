package com.bl.bigdata.service

import com.bl.bigdata.bean.Polygon
import com.infomatiq.jsi.Rectangle;

trait  MBRService extends Serializable{
	
	/**
	 * get MBR of polygon 
	 * @param polygon
	 * @return
	 */
	def  getMBROfPolygon(polygon: Polygon) : Rectangle;

}
