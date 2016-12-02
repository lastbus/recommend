package com.bl.bigdata.search

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by MK33 on 2016/10/9.
  */
object ExportOrderDataToHbase {

  def main(args: Array[String]) {
    val s = "select * " +
      " from s03_oms_order_detail o  " +
      " join sourcedata.s03_oms_order_sub o2 on o.order_no = o2.order_no " +
      " where o2.recept_address_detail is not null "

    val addressSql = "select * " +
      " from s03_oms_order_detail o  " +
      " join sourcedata.s03_oms_order_sub o2 on o.order_no = o2.order_no " +
      " join addressCoordinate a on a.address = replace(o2.recept_address_detail, ' ', '')   " +
      " where o2.recept_address_detail is not null "



    val sc = new SparkContext(new SparkConf())
    val hiveContext = new HiveContext(sc)
    val orderDF = hiveContext.sql(s)
   val columnArray = orderDF.columns

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "order")
    hbaseConf.set("hbase.zookeeper.quorum", "m79sit,s80sit,s81sit")

    val job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[Put]])

    val putRDD = orderDF.map{ row =>
      val featureBytes = Bytes.toBytes("feature")
      val length = row.length
      val orderNo = row.getString(16)
      val goodsCode = row.getString(20)
      val put = new Put(Bytes.toBytes(orderNo + "-" + goodsCode))
//      val schema = row.schema
      for (i <- 0 until length) {
        if (!row.isNullAt(i))
          put.addColumn(featureBytes, Bytes.toBytes(columnArray(i)), Bytes.toBytes(row.get(i).toString))
      }
      (new ImmutableBytesWritable(Bytes.toBytes("")), put)
    }
    putRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)


  }

  def getAddressRDD(sc: SparkContext): Unit = {
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set(TableInputFormat.INPUT_TABLE, "order")

    val rawHBaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2)
    rawHBaseRDD.map{ r =>
      val coordinateBytes = Bytes.toBytes("coordinate")
      val address  = Bytes.toString(r.getRow)

      val longitude = r.getValue(coordinateBytes, Bytes.toBytes("longitude"))
      val latitude = r.getValue(coordinateBytes, Bytes.toBytes("latitude"))
      val formattedAddress = r.getValue(coordinateBytes, Bytes.toBytes("formatted_address"))
      val province = r.getValue(coordinateBytes, Bytes.toBytes("province"))
      val city = r.getValue(coordinateBytes, Bytes.toBytes("city"))
      val cityCode = r.getValue(coordinateBytes, Bytes.toBytes("citycode"))
      val district = r.getValue(coordinateBytes, Bytes.toBytes("district"))
      val adCode = r.getValue(coordinateBytes, Bytes.toBytes("adcode"))

      ()


    }


  }

}
