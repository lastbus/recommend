package com.bl.bigdata

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK33 on 2016/10/17.
  */
object OrderAndStoreAnalysis {

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("store order analysis"))

    val storeAndOrderDetailSql =
      """
        | SELECT address.storename, address.storelocation, address.district,
        | o.order_no, o.goods_code, o.goods_name, o.brand_sid, o.brand_name, o.sale_price, o.sale_sum,
        | o2.member_id,
        |  g2.category_id, g2.category_name
        |  FROM sourcedata.s03_oms_order_detail  o
        |  JOIN sourcedata.s03_oms_order o2 ON o2.order_no = o.order_no
        |  JOIN sourcedata.s03_oms_order_sub sub ON sub.order_no = o.order_no
        |  JOIN address_coordinate_store  address ON address.address = regexp_replace(sub.recept_address_detail, ' ', '')
        |  JOIN (SELECT DISTINCT g.sid, g.category_id, g.category_name  FROM recommendation.goods_avaialbe_for_sale_channel g ) g2 ON g2.sid = o.goods_code
        |  WHERE address.city = '上海市' and o2.order_type_code <> '25' and o2.order_status = '1007'
      """.stripMargin
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import hiveContext.implicits._
    val orderDetailDF = hiveContext.sql(storeAndOrderDetailSql)
    orderDetailDF.cache()
    orderDetailDF.registerTempTable("tmp")

    val (salesSum, salesAmt) = hiveContext.sql("select sum(sale_price * sale_sum), sum(sale_sum) from tmp ").rdd.map(r => (r.getDouble(0), r.getDouble(1))).take(1)(0)

    // 门店周围用户数量
    val memberDF = orderDetailDF.select($"storename", $"storelocation", $"member_id").
      rdd.filter(!_.anyNull).map(row => ((row.getString(0), row.getString(1)), Set(row.getString(2)))).
      reduceByKey(_ ++ _).mapValues(_.size)
    memberDF.cache()

    memberDF.sortBy(_._2, false).map(s => s"${s._1._1},${s._1._2},${s._2}").coalesce(1).saveAsTextFile("/tmp/store_member_count")

    // 线下快到家门店周围热销品类
    val categoryDF = orderDetailDF.select($"storename", $"storelocation", $"category_id", $"category_name", $"sale_price" * $"sale_sum").
      rdd.filter(!_.anyNull).map(row => ((row.getString(0), row.getString(1)), (row.get(2).toString, row.getString(3), row.getDouble(4)))).
      map (s => (s._1, Seq(s._2))).reduceByKey(_ ++ _).
      map { case ((store, location), categories) =>
        ((store, location), categories.groupBy(s => (s._1, s._2)).map(s => (s._1, s._2.length, s._2.foldLeft(0.0)((a: Double, b: (String, String, Double)) => a + b._3))).
          toArray.sortWith(_._2 > _._2).take(100).map(s => s._1._2 + ":" + s._2 / salesAmt + ":" + s._2 + ":" + s._3 / salesSum + ":" + s._3).mkString(" || "))
      }
    categoryDF.cache

    categoryDF.map{ s => s"${s._1._1},${s._1._2},${s._2}"}

    categoryDF.takeSample(false, 10).foreach(s => Console println s._1 + "   " + s._2)

    // 线下快到家门店周围热销商品，排序的依据可以是销量，也可以是销售额
    val goodsDF = orderDetailDF.select($"storename", $"storelocation", $"goods_code", $"goods_name", $"sale_price" * $"sale_sum").
      rdd.filter(!_.anyNull).map(row => ((row.getString(0), row.getString(1)), (row.get(2).toString, row.getString(3), row.getDouble(4)))).
      map(s => (s._1, Seq(s._2))).reduceByKey(_ ++ _).
      map { case ((store, location), categories) =>
        ((store, location), categories.groupBy(s => (s._1, s._2)).map(s => (s._1, s._2.length, s._2.foldLeft(0.0)((a: Double, b: (String, String, Double)) => a + b._3))).
          toArray.sortWith(_._2 > _._2).take(100).map(s => s._1._2 + ":" + s._2 / salesAmt + ":" + s._2 + ":" + s._3 / salesSum + ":" + s._3).mkString(" || "))
      }
    goodsDF.cache()

    goodsDF.takeSample(false, 5).foreach(s => Console println s._1._1 + "   " + s._1._2 + "\n" + s._2.mkString("\n") + "\n")


    // 线下快到家门店周围热销的品牌
    val brandDF = orderDetailDF.select($"storename", $"storelocation", $"brand_sid", $"brand_name", $"sale_price" * $"sale_sum").
      rdd.filter(!_.anyNull).map(row => ((row.getString(0), row.getString(1)), (row.get(2).toString, row.getString(3), row.getDouble(4)))).
      map(s => (s._1, Seq(s._2))).reduceByKey(_ ++ _).
      map { case ((store, location), categories) =>
        ((store, location), categories.groupBy(s => (s._1, s._2)).map(s => (s._1, s._2.length, s._2.foldLeft(0.0)((a: Double, b: (String, String, Double)) => a + b._3))).
          toArray.sortWith(_._3 > _._3).take(100).map(t => t._1._2 + ":" + t._2 / salesAmt + ":" + t._2 + ":" + t._3 / salesSum + ":" + t._3).mkString(" || "))
      }

    brandDF.cache

    brandDF.map(s=> s"${s._1._1},${s._1._2},${s._2}" ).coalesce(1)

    brandDF.takeSample(false, 5).foreach(s => Console println s._1 + "   " + s._2)

    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum", "slave23.bl.bigdata,slave24.bl.bigdata,slave21.bl.bigdata,slave22.bl.bigdata,slave25.bl.bigdata")
    val admin = new HBaseAdmin(hBaseConf)
    val tableName = "order_store_tmp_result"
    if (!admin.tableExists(tableName)) {
      val hTableDescriptor = new HTableDescriptor(tableName)
      hTableDescriptor.addFamily(new HColumnDescriptor("info"))
      admin.createTable(hTableDescriptor)
    }

    val job = Job.getInstance(hBaseConf)

    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[Put])
    job.setOutputFormatClass(classOf[TableOutputFormat[Put]])

    brandDF.map { row =>
      val put = new Put(Bytes.toBytes(""))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("brand"), Bytes.toBytes(row._2.mkString("|")))
      (new ImmutableBytesWritable(Bytes.toBytes("")), put)
    }.foreachPartition { partition =>
      val hBaseConf2 = HBaseConfiguration.create()
      hBaseConf2.set("hbase.zookeeper.quorum", "slave23.bl.bigdata,slave24.bl.bigdata,slave21.bl.bigdata,slave22.bl.bigdata,slave25.bl.bigdata")
        val conn = ConnectionFactory.createConnection(hBaseConf2)
        val table = conn.getTable(TableName.valueOf(tableName))
        partition.foreach(s =>table.put(s._2))
      conn.close()
    }


    // 计算价格带
    val sql1 = "select category_id, sale_price, category_name  from recommendation.goods_avaialbe_for_sale_channel  "
    val goodsRawRDD2= hiveContext.sql(sql1).map { row =>
      if (row.anyNull) null else (row.getLong(0).toString , row.getDouble(1), row.getString(2))
    }
    val validateGoodsRDD = goodsRawRDD2.filter(_!=null)
    val priceZoneRDD = validateGoodsRDD.map(s => ((s._1, s._3), Seq(s._2))).reduceByKey(_ ++ _).mapValues(s => (s.min, s.max))
    val fileName = "/tmp/category_price_zone"
    priceZoneRDD.map{ case ((cate, name), (min, max)) =>
      val delt = (max - min) / 3
      val r = for (i <- 0 until 3) yield {
        min + delt * i
      }
      s"$cate,$min,$max,${r.mkString(",")},$max,$name"
    }.coalesce(1).saveAsTextFile(fileName)
    // 类别，最低价格，最高价格，价格带
    val priceZone = sc.textFile(fileName).map(line => {val s = line.split(","); (s(0), (s(1), s(2), Array(s(3).toDouble, s(4).toDouble, s(5).toDouble, s(6).toDouble)))})

    val goodsPriceRDD = orderDetailDF.select($"storename", $"storelocation", $"sale_price", $"category_id", $"category_name").
      rdd.map { row =>
      if (row.anyNull || row.isNullAt(3) || row.get(3).toString.equalsIgnoreCase("NULL")) null
      else (row.getLong(3).toString, (row.getString(0), row.getString(1), row.getDouble(2)))
    }.filter(_ != null).join(priceZone).map { case (category, ((storeName, storeLocation, price), (min, max, pricesArray))) =>
        val level = if (price < pricesArray(1)) "L" else if (price < pricesArray(2)) "M" else "H"
      ((storeName, storeLocation), Seq(level))
    }.reduceByKey(_ ++ _).map { case ((storeName, storeLocation), levels) =>
      val size = levels.length.toDouble
      ((storeName, storeLocation), (levels.filter(_ == "H").length / size, levels.filter(_ == "M").length / size, levels.filter(_ == "L").length / size))
    }

    goodsPriceRDD.take(1)
//    goodsPriceRDD.top(100)(Ordering.by(_._3)).foreach(println)
//    goodsPriceRDD.sortBy(_._3, false).map(s=> s"${s._1},${s._2},${s._3},${s._4}").coalesce(1).saveAsTextFile("/tmp/store_price_zone_distibution")


    // 将特征按照门店放在一起
    val result = memberDF.join(goodsPriceRDD).map { case (k, (v1, v2)) => (k, (v1,v2))}.
      join(categoryDF).map { case (k, ((v1, v2), v3)) => (k, (v1, v2, v3))}.
      join(brandDF).map { case (k, ((v1, v2, v3), v4)) => (k, (v1, v2, v3, v4))}.
      join(goodsDF).map { case (k, ((v1, v2, v3, v4), v5)) => (k, v1, v2, v3, v4, v5)}

    result.map { case (k, v1, v2, v3, v4, v5) =>
        s"${k._1},${k._2},${v1},${v2._1},${v2._2},${v2._3},${v3},${v4},${v5}"
    }.coalesce(1).saveAsTextFile("/tmp/store_result2")



  }

}
