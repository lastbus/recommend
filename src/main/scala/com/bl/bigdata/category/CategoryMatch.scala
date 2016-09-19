package com.bl.bigdata.category


import java.sql.{DriverManager, PreparedStatement, Connection}

import com.bl.bigdata.util.SparkFactory
import com.rockymadden.stringmetric.similarity.JaccardMetric
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/9/7.
 */
class CategoryMatch {

  def yhdCategoryMatch(): Unit = {

    val sc = SparkFactory.getSparkContext("category_match")
    val hiveContext = new HiveContext(sc)
    val cateSql = "SELECT distinct category_id,category_name FROM  idmdata.dim_management_category where category_name is not null and category_id is not null"
    val cateRdd = hiveContext.sql(cateSql).rdd.map(row => (row.getLong(0), row.getString(1)))

    val url = "jdbc:mysql://10.201.129.74:3306/recommend_system?user=root&password=bl.com"
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val yhdCateDf = sqlContext.jdbc(url, "YHD_CATEGORY")
    val yhdCateRdd = yhdCateDf.map(row => {
      (row.getString(1), row.getString(2))
    }).map { case (category_name, category_sid) => {
      val cnArray = category_name.split(">")
      val cn = cnArray(cnArray.length - 1)
      (cn,category_name, category_sid)
    }
    }

    val matCateRdd = cateRdd.cartesian(yhdCateRdd).map { case (bCate,yCate) => {
      var sim = JaccardMetric(1).compare(bCate._2,yCate._1)
      var simv = 0.0
      if(!sim.isEmpty)
        simv = sim.get
      (bCate,Seq((yCate,simv)))
    }
    }.reduceByKey( _ ++ _).mapValues(v=>v.sortWith((a,b)=>a._2 > b._2)).mapValues(v=>v.take(1))

    var conn: Connection = null
    var ps: PreparedStatement = null
    val usql  = "update bl_category_performance_basic  SET yhd_category_tree = ?,yhd_category_url = ? where category_sid =?"
    try {
      matCateRdd.foreachPartition(partition => {
        val driver = "com.mysql.jdbc.Driver"
        Class.forName(driver)
        conn = DriverManager.getConnection("jdbc:mysql://10.201.129.74:3306/recommend_system", "root", "bl.com")
        partition.foreach { data =>
          ps = conn.prepareStatement(usql)
          ps.setString(1,data._2(0)._1._2)
          ps.setString(2, data._2(0)._1._3)
          ps.setInt(3, data._1._1.toInt)

          ps.executeUpdate()
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

object CategoryMatch {
  def main(args: Array[String]): Unit = {
    val cm = new CategoryMatch
    cm.yhdCategoryMatch()
  }
}
