package com.bl.bigdata.mail

/**
  * Created by MK33 on 2016/3/31.
  */
trait Item

case class UserBehaviorRawData1(cookieID: String,
                               memberID: String,
                               sessionID: String,
                               goodsSID: String,
                               goodsName: String,
                               quality: String,
                               eventDate: String,
                               behaviorType: String,
                               channel: String,
                               categorySID: String,
                               dt: String) extends Item

case class UserBehaviorRawData(cookieID: String,
                               memberID: String,
                               sessionID: String,
                               goodsID: String,
                               goodsName: String,
                               salePrice: String,
                               saleSum: String,
                               saleTime: String,
                               orderSource: String,
                               categoryID: String,
                               categoryName: String,
                               brandSID: String,
                               brandName: String,
                               orderNo: String,
                               dt: String) extends Item
