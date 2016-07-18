package com.bl.bigdata.recommend

import java.net.URLDecoder

import com.bl.bigdata.util.{RedisClient, SparkFactory}
import com.rockymadden.stringmetric.similarity.LevenshteinMetric
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by HJT20 on 2016/5/3.
 */
class SearchKeyWords {

  def searchHotWordsToRedis(): Unit = {
    val sc = SparkFactory.getSparkContext("search_hot_words")
    val hiveContext = new HiveContext(sc)
    val sql = "select onsite_search_word from recommendation.search_hot_keywords"
    val hotWords = hiveContext.sql(sql).rdd.map(row => row.getString(0)).collect()
    val words = hotWords.mkString("#")
    val jedisCluster = RedisClient.jedisCluster
    jedisCluster.set("search_index_hotwords", words)
  //  jedisCluster.close()
  //  sc.stop()
  }

  def categoryWords(): Unit = {
    val sc = SparkFactory.getSparkContext("term_category")
    val hiveContext = new HiveContext(sc)
    val sql = "select search_sid,name,keyword,count from recommendation.term_category"
    val cateWordRdd = hiveContext.sql(sql).rdd.map(row => ((row.getString(0), URLDecoder.decode(row.getString(2), "UTF8").trim.toLowerCase()), row.getLong(3)))
    val wcRdd = cateWordRdd.reduceByKey(_ + _).map { x => {
      (x._1._1, (x._1._2, x._2))
    }
    }
    val cateWordsRdd = wcRdd.mapValues(p => Set(p)).reduceByKey((s1, s2) => s1.++(s2))
    val sortedWordsRdd = cateWordsRdd.mapValues(s => s.toArray.sortWith(_._2 > _._2))
    val tRdd = sortedWordsRdd.mapValues(s => {
      var outw = ""
      s.map {
        w => outw = outw + w._1 + "#"
      }
      outw
    }
    )
    tRdd.foreachPartition(partition => {
      val jedis = RedisClient.jedisCluster
      partition.foreach(s => {
        //println(s._1.toString+"->"+ s._2)
        jedis.set("search_cate_hotwords_" + s._1.toString, s._2)
      })
      //jedis.close()
    })
   // sc.stop()
  }

  def termCategoris(): Unit = {
    val sc = SparkFactory.getSparkContext("term_category")
    val hiveContext = new HiveContext(sc)
    val sql = "select search_sid,name,keyword,count from recommendation.term_category where count>10"
    //keyword  search_sid count
    val cateWordRdd = hiveContext.sql(sql).rdd.map(row => ((URLDecoder.decode(row.getString(2).toLowerCase.trim, "UTF8"), row.getString(0), row.getString(1)), row.getLong(3)))
    val wcRdd = cateWordRdd.reduceByKey(_ + _).map { x => {
      (x._1._1, (x._1._2, x._1._3, x._2))
    }
    }
    val cateWordsRdd = wcRdd.mapValues(p => Set(p)).reduceByKey((s1, s2) => s1.++(s2))
    val sortedWordsRdd = cateWordsRdd.mapValues(s => s.toArray.sortWith(_._3 > _._3))
    val tRdd = sortedWordsRdd.mapValues(s => {
      var outw = ""
      s.map {
        w => outw = outw + w._1 + ":" + w._2 + "#"
      }
      outw
    }
    )

    tRdd.foreachPartition(partition => {
      val jedis = RedisClient.jedisCluster
      partition.foreach(s => {
        jedis.set("search_term_category_" + s._1.toString.toLowerCase, s._2)
      })
     // jedis.close()
    })
   // sc.stop()

  }

  def termSim(): Unit = {
    val sc = SparkFactory.getSparkContext("term_sim")
    val hiveContext = new HiveContext(sc)
    val sql = "select distinct keyword from recommendation.term_category where count>5"
    val termRdd = hiveContext.sql(sql).rdd.map(row => URLDecoder.decode(row.getString(0).trim.toLowerCase.replace(" ", ""), "UTF8")).filter(_.trim.length > 0)
    termRdd.foreachPartition(partition => {
      val jedis = RedisClient.jedisCluster
      partition.foreach(s => {
        var termList: List[String] = List[String]()
        val catCnt = jedis.get("search_term_category_" + s)
        if (catCnt != null) {
          val catArray = catCnt.split("#").slice(0, 3)
          for (cat <- catArray) {
            val catId = cat.split(":")(0)
            val terms = jedis.get("search_cate_hotwords_" + catId)
            val ta = terms.split("#").slice(0, 20)
            for (t <- ta) {
              if (t.trim.length != 0 && !termList.contains(t.trim.toLowerCase()) && s.trim.toLowerCase().compareTo(t.trim.toLowerCase()) != 0) {
                termList = termList.:+(t.trim.toLowerCase)
              }
            }
          } //for
        }

        val a: List[(Int, String)] = for (rs <- termList; dist = LevenshteinMetric.compare(s, rs)) yield (dist.get, rs)
        val rvalue = a.distinct.sortWith(_._1 < _._1).filter(_._1 != 0).slice(0, 10).map { case (a, b) => b }.mkString("#")
        jedis.set("search_term_sim_" + s, rvalue)
      })
 //     jedis.close()
    })
    //sc.stop()
  }

  def dimCategory(): Unit = {
    val sc = SparkFactory.getSparkContext("dim_search_category")
    val hiveContext = new HiveContext(sc)
    val sql = "select lev1_sid,lev2_sid,lev3_sid,lev4_sid,lev5_sid,id from  recommendation.dim_search_category"
    val cateRdd = hiveContext.sql(sql).rdd.map(row => Set((row.get(0), row.get(5)), (row.get(1), row.get(5)), (row.get(2), row.get(5)),
      (row.get(3), row.get(5)),(row.get(4), row.get(5))
    ))
    val subCateRdd = cateRdd.flatMap(s => (s.map(x => x))).filter(x=>x._1!=null)
    val resRdd = subCateRdd.mapValues(x => Set(x.toString)).reduceByKey(_ ++ _).mapValues(x => x.mkString("#")).filter(_._1 != null)
    resRdd.foreachPartition(partition => {
      val jedis = RedisClient.jedisCluster
      partition.foreach(s => {
        if( s._1.toString.compareTo(s._2)!=0) {
          jedis.set("search_sub_sch_cate_" + s._1.toString, s._2)
        }
      })
    //  jedis.close()
    })
    sc.stop()
  }
}

object SearchKeyWords {
  def main(args: Array[String]) {
    val skw = new SearchKeyWords
    skw.searchHotWordsToRedis
    skw.categoryWords
    skw.termCategoris
    skw.termSim
    skw.dimCategory
  }
}