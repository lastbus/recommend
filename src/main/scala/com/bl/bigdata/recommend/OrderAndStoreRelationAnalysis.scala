package com.bl.bigdata.recommend


import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

/**
  * Created by MK33 on 2016/10/14.
  */
object OrderAndStoreRelationAnalysis {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("test"))
    val hiveContext = new HiveContext(sc)
    val sql = "SELECT a0.storename, a0.storelocation, a0.catesid, a0.goodscode, a0.goodsname, sum(a0.saleprice * a0.salesum),  " +
      "sum(a0.salesum), sum(a0.saleprice * a0.salesum) / sum(a0.salesum),   " +
      "sum(a0.saleprice * a0.salesum) / count(DISTINCT a0.orderno),  " +
      "sum(a0.saleprice * a0.salesum) / count(DISTINCT a0.memberid),count(DISTINCT a0.memberid)   " +
      "FROM addresscoordinatestore a0  GROUP BY a0.storename, a0.storelocation, a0.catesid,a0.goodscode, a0.goodsname   "

    val a = hiveContext.sql(sql)

    a.rdd.zipWithIndex().foreachPartition { partition =>
      val hHbaseConf = HBaseConfiguration.create()
      hHbaseConf.set("hbase.zookeeper.quorum", "m79sit,s80sit,s81sit")
      val conn = ConnectionFactory.createConnection(hHbaseConf)
      val table = conn.getTable(TableName.valueOf("r0"))
      val columnFamilyBytes = Bytes.toBytes("info")
      partition.foreach { case (row, index) =>
        val put = new Put(Bytes.toBytes(index))
          put.addColumn(columnFamilyBytes, Bytes.toBytes("storeName"), Bytes.toBytes(row.getString(0)))
        put.addColumn(columnFamilyBytes, Bytes.toBytes("storeLocation"), Bytes.toBytes(row.getString(1)))
        put.addColumn(columnFamilyBytes, Bytes.toBytes("categoryId"), Bytes.toBytes(if (row.isNullAt(2)) "not-know" else row.getString(2)))
        put.addColumn(columnFamilyBytes, Bytes.toBytes("goodsId"), Bytes.toBytes(row.getString(3)))
        put.addColumn(columnFamilyBytes, Bytes.toBytes("goodsName"), Bytes.toBytes(row.getString(4)))
        put.addColumn(columnFamilyBytes, Bytes.toBytes("sales"), Bytes.toBytes(if (row.isNullAt(5) || row.get(5).toString.equals("null")) 0.0 else row.getDouble(5)))
        put.addColumn(columnFamilyBytes, Bytes.toBytes("saleAmt"), Bytes.toBytes(if (row.isNullAt(6) || row.get(6).toString.equals("null")) 0 else row.getDouble(6).toInt))
        put.addColumn(columnFamilyBytes, Bytes.toBytes("avgSales"), Bytes.toBytes(if (row.isNullAt(7) || row.get(7).toString.equals("null")) 0.0 else row.getDouble(7)))
        put.addColumn(columnFamilyBytes, Bytes.toBytes("orderAvgSales"), Bytes.toBytes(if (row.isNullAt(8) || row.get(8).toString.equals("null")) 0.0 else row.getDouble(8)))
        put.addColumn(columnFamilyBytes, Bytes.toBytes("salesPerCustom"), Bytes.toBytes(if (row.isNullAt(9) || row.get(9).toString.equals("null")) 0.0 else row.getDouble(9)))
        put.addColumn(columnFamilyBytes, Bytes.toBytes("memberCount"), Bytes.toBytes(if (row.isNullAt(10) || row.get(10).toString.equals("null")) 0 else row.getLong(10).toInt))
          table.put(put)
      }
      if (table != null) {
        table.close()
      }
      if (conn != null) conn.close()

    }

//    val b = a.rdd.map{ row =>
//      val json = new JSONObject()
//      json.put("storeName", row.getString(0))
//      json.put("storeLocation", row.getString(1))
//      json.put("categoryId", row.getString(2))
//      json.put("goodsId", row.getString(3))
//      json.put("goodsName", row.getString(4))
//      json.put("sales", if (row.isNullAt(5)) 0.0 else row.getDouble(5))
//      json.put("saleAmt", if (row.isNullAt(6)) 0 else row.getDouble(6).toInt)
//      json.put("avgSales", if (row.isNullAt(7)) 0.0 else row.getDouble(7))
//      json.put("orderAvgSales", if (row.isNullAt(8)) 0.0 else row.getDouble(8))
//      json.put("salesPerCustom", if (row.isNullAt(9)) 0.0 else row.getDouble(9))
//      json.put("memberCount", if (row.isNullAt(10)) 0 else row.getLong(10).toInt)
//      json.toString
//    }
//      b.saveAsTextFile("/tmp/order_store_result.json")




  }

}
