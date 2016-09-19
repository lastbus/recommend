package com.bl.bigdata.category

import java.sql.{DriverManager, PreparedStatement, Connection}

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by HJT20 on 2016/9/18.
 */
class CategoryBLYhdBrandsContrast {

  def brandsContrast(): Unit ={

    val sparkConf = new SparkConf().setAppName("category_pop")
    val sc = new SparkContext(sparkConf)
    val url = "jdbc:mysql://10.201.129.74:3306/recommend_system?user=root&password=bl.com"
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val blBrandDf = sqlContext.jdbc(url, "bl_category_performance_category_brand")
    val blBrdRdd = blBrandDf.map(row=>{
      (row.getInt(0),Seq(row.getString(3)))
    }).reduceByKey(_ ++ _)

    val yhdBrandDf = sqlContext.jdbc(url, "YHD_CATEGORY").distinct
    val yhdBrdRdd = yhdBrandDf.map(row=>{
      (row.getString(2),row.getString(5))
    }).mapValues(brd=>{
      val brands = brd.split("#").filter(!_.isEmpty).toSeq
      brands.map(ze =>{
        ze.split("/")(0).trim.replace(" ","").toLowerCase()
      })
    })

    val blYhdBrandDf = sqlContext.jdbc(url, "bl_category_performance_basic").distinct
    val blYhdBrdMapRdd = blYhdBrandDf.map(row=>{
      (row.getInt(0), if (row.isNullAt(12) || row.get(12).toString.equalsIgnoreCase("null")) null else row.getString(12))
    }).filter(_._2 != null)

    val blYhdBrdRdd =  blYhdBrdMapRdd.join(blBrdRdd).map(x=>{
      val blCateId = x._1
      val yhdCateId = x._2._1
      val blBrds = x._2._2
      (yhdCateId,(blCateId,blBrds))
    }).join(yhdBrdRdd).map(x=>{
      val yhdCateId = x._1
      val blCateId = x._2._1._1
      val blBrds = x._2._1._2
      val yhdBrds = x._2._2
      val mbyb = yhdBrds.filter(!blBrds.contains(_))
      ((blCateId,yhdCateId), mbyb)
    }).filter(_._2.size >1).flatMapValues(x=>x)


    var conn: Connection = null
    var ps: PreparedStatement = null
    val msql = "insert into bl_category_performance_bl_yhd_brand_contrast(category_sid,yhd_cate_url,yhd_brand)values (?, ?,?)"
    try {
      blYhdBrdRdd.foreachPartition(partition => {
        val driver = "com.mysql.jdbc.Driver"
        Class.forName(driver)
        conn = DriverManager.getConnection("jdbc:mysql://10.201.129.74:3306/recommend_system", "root", "bl.com")
        partition.foreach { data => {
          ps = conn.prepareStatement(msql)
          ps.setInt(1, data._1._1)
          ps.setString(2, data._1._2)
          ps.setString(3, data._2)

          ps.executeUpdate()
        }
        }
      }
      )
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }




  }

}


