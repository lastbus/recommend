package com.bl.bigdata.config

/**
 * Created by HJT20 on 2016/5/30.
 */
object HbaseConfig {

  val test = true
  var  hbase_zookeeper_property_clientPort:String = "2181"
  var  hbase_zookeeper_quorum:String =  "slave23.bl.bigdata,slave24.bl.bigdata,slave21.bl.bigdata"
  var  hbase_master = "10.201.48.27:60000"
  if(test)
  {
        hbase_zookeeper_property_clientPort = "2181"
        hbase_zookeeper_quorum =  "10.201.129.81"
        hbase_master = "10.201.129.78:60000"

  }
  val goodstbl ="goods_profile"
  val similar_goods_familay = "similar_goods"
  val similar_goods_property_based_qualifier = "property_based_in_category"
  val similar_goods_view_based_in_category = "view_based_in_category"
  val similar_goods_shop_based_in_category = "shop_based_in_category"
  val basic_info_family = "basic_info"
  //create 'goods_profile','basic_info',similar_goods','profiles'

  //用户
  val user_profile="user_profile"
  val user_profile_interests_family="interests"
  val  user_profile_interests_category_qualifer="category"
  val user_profile_behavior_characteristics_family = "behavior_characteristics"
  val user_profile_behavior_characteristics_last_two_month_browsed_qualifier = "last_two_mont_browsed"
  val user_profile_behavior_characteristics_last_two_month_shoped_qualifier = "last_two_mont_shoped"

  //推荐
  val user_cf_input_table="user_cf_taste"


}
