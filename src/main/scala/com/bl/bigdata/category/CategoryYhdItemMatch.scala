package com.bl.bigdata.category

import com.bl.bigdata.util.SparkFactory
import com.rockymadden.stringmetric.similarity.JaccardMetric
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/9/8.
 */
class CategoryYhdItemMatch {
def itemmatch(): Unit ={
  val sc = SparkFactory.getSparkContext("category_match")

  val url = "jdbc:mysql://10.201.129.74:3306/recommend_system?user=root&password=bl.com"
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val yhdItemDf = sqlContext.jdbc(url, "yhd_items")
  val topYhdItems = yhdItemDf.map(row=>{
   val comment = row.getInt(5)
    val cateUrl =row.getString(1)
    (cateUrl,Seq((comment,row)))
  }).reduceByKey(_ ++ _).mapValues(x=>x.sortWith((a,b)=>a._1>b._1)).mapValues(x=>x.take(50)).mapValues(x=> {
      x.map { case (cmt, row) => row
      }
    }).flatMapValues(x=>x)



  val blYhdCate = sqlContext.jdbc(url,"bl_category_performance_basic")
  blYhdCate.registerTempTable("blYhdCateTbl")
  val matCate = sqlContext.sql("select category_sid,yhd_category_url from blYhdCateTbl where yhd_category_url is not null")
  val byMateCateRdd = matCate.map(row=>(row.getInt(0),row.getString(1)))



  val hiveContext = new HiveContext(sc)
  val BLGoodsSql = "SELECT DISTINCT category_id,sid,goods_sales_name,cn_name,sale_price FROM recommendation.goods_avaialbe_for_sale_channel WHERE sale_status=4 and category_id is not null"
  val BLGoodsRdd = hiveContext.sql(BLGoodsSql).rdd.map(row => (row.getLong(0).toInt, row))

  val blYhdRdd = byMateCateRdd.join(BLGoodsRdd).map(x=>{
    val blcateId = x._1
    val yhdUrl = x._2._1
    val blgoods = x._2._2
    (yhdUrl,(blcateId,blgoods))
  }).join(topYhdItems)

    val mmd = blYhdRdd.map(x=>{
    val yhdUrl = x._1
    val yhdItem = x._2._2
    val yhdName = yhdItem.getString(3)
    val yhdItemUrl = yhdItem.getString(2)
    val yhdItemPrice = yhdItem.getDouble(4)

    val blCateId = x._2._1._1
    val blgoods = x._2._1._2
    val blGoodsId =blgoods.getString(1)
    val blGoodsName = blgoods.getString(2)
    val blBrand = blgoods.getString(3)
    val blGoodsPrice = blgoods.getDouble(4)
      (yhdName,blGoodsName,blBrand)
      //同品牌比较
    if(yhdName.contains(blBrand))
      {
        var tmpYhdName = yhdName.replaceAll(blBrand,"")
        var tmpBlName = blGoodsName.replaceAll(blBrand,"")
        val regEx1 = "[\\u4e00-\\u9fa5||()]"
        val regEx2 = "[a-z||A-Z||0-9]"
        val tmpYhdWords = tmpYhdName.replaceAll(regEx2,"")
        val tmpBlWords =  tmpBlName.replaceAll(regEx2,"")
        val tmpYhdad =  tmpYhdName.replaceAll(regEx1," ")
        val tmpBlad = tmpBlName.replaceAll(regEx1," ")
        var wc = 0
        tmpBlWords.foreach(ch=>{
          if(tmpYhdWords.contains(ch))
            {
              wc += 1
            }
        })

        val adArray1 = tmpBlad.split(" ")
        val adArray2 = tmpYhdad.split(" ")
        var strc = 0
        adArray1.foreach(str=>{
          if(adArray2.contains(str))
            {
              strc += 1
            }
        })

        (yhdName,blGoodsName,tmpYhdWords,tmpBlWords,tmpYhdad,tmpBlad,strc,wc)
      }

  }).filter(_ != null)


//  val matItems = BYItems.map(x=>{
//    val yhd_url = x._1
//    val bl_item = x._2._1
//    val yhd_item = x._2._2
//    val bl_name = bl_item.getString(5)
//    val yhd_name = yhd_item.getString(3)
//    var sim = JaccardMetric(1).compare(bl_name,yhd_name)
//    var simv = 0.0
//    if(!sim.isEmpty)
//      simv = sim.get
//    (bl_item,Seq((yhd_item,simv)))
//  }).reduceByKey(_ ++ _).mapValues(x=>x.sortWith((a,b)=>a._2>b._2)).mapValues(x=>x.take(1))
//
//  matItems.saveAsTextFile("/home/hdfs/matItems")


}
}

object CategoryYhdItemMatch {
  def main(args: Array[String]) {
 val cym = new CategoryYhdItemMatch
    cym.itemmatch()
  }
}
