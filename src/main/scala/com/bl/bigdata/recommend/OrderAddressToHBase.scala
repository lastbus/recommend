package com.bl.bigdata.recommend

import java.io.BufferedReader
import java.net.URL

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

/**
  * 调用高德地图接口，查询地址的经纬度
  * Created by MK33 on 2016/10/8.
  */
object OrderAddressToHBase {

  val logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    val s = "select distinct o.order_no, o2.recept_address_detail  " +
      " from s03_oms_order_detail o  " +
      " join sourcedata.s03_oms_order_sub o2 on o.order_no = o2.order_no " +
      " where o2.recept_address_detail is not null "



    val sc = new SparkContext(new SparkConf())
    val addressRDD = sc.textFile("hdfs://m78sit:8020/tmp/address")

    addressRDD.cache()
//    println(" =============  " + addressRDD.map(line => line.split("\t")(1).replace(" ", "")).distinct().count()  + "  ===================")
//    println(" =============  " + addressRDD.map(line => line.split("\t")(1)).distinct().count()  + "  ===================")
    addressRDD.map(line => (line.split("\t")(1), 1)).distinct().subtract(addressRDD.map(line => (line.split("\t")(1).replace(" ", ""), 1)).distinct())
        .map(_._1.replace(" ", "")).sortBy(s => s).foreach(println)

    sys.exit()

    addressRDD.foreachPartition { partition =>

      val baseUrl = "http://restapi.amap.com/v3/geocode/geo"
      val key = "02ea79be41a433373fc8708a00a2c0fa"
//      val key = "47ceb1d05c0e8d7e20e5fdcf90f83246"
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "m79sit,s80sit,s81sit")
      val hBaseConn = ConnectionFactory.createConnection(conf)
      val table = hBaseConn.getTable(TableName.valueOf("addressCoordinate"))
      val table2 = hBaseConn.getTable(TableName.valueOf("addressCoordinate_error"))

      partition.foreach { line =>
        val orderAndAddress = line.split("\t")
        val orderNo = orderAndAddress(0)
        val address = orderAndAddress(1).replace(" ", "")
        logger.debug(orderNo + "  " + address)
        // 当地址有效，并且hbase中不存在这条记录，则去查找，否则不查找
        if (address != null && address.length > 0 && !table.exists(new Get(Bytes.toBytes(address)))) {
          val url = s"$baseUrl?key=$key&address=$address"
          logger.debug(orderNo + "  " + url)

          val buffer =getConnection(url, 3)
          val sb = new StringBuilder
          var reader = buffer.readLine()
          while (reader != null) {
            sb.append(reader)
            reader = buffer.readLine()
          }
          println(sb.toString())
          val json = new JSONObject(sb.toString())
          json.getString("status") match {
            case "0" =>
              val put = new Put(Bytes.toBytes(address))
              put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("order_no"), Bytes.toBytes(orderNo))
              put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(sb.toString()))
              table2.put(put)
              logger.error("request failed, failed info: " + json.getString("info"))
            case "1" =>
              json.getString("infocode") match {
                case "10000" => val count = json.getString("count").toInt
                  if (count > 1) {
                    val put = new Put(Bytes.toBytes(address))
                    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("order_no"), Bytes.toBytes(orderNo))
                    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(sb.toString()))
                    table2.put(put)
                    logger.info("more than on coordinates: " + sb.toString())
                  } else if (count == 1) {
                    val locationJson = json.getJSONArray("geocodes").getJSONObject(0)
                    val location = locationJson.getString("location").split(",")
                    val put = new Put(Bytes.toBytes(address))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("longitude"), Bytes.toBytes(location(0)))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("latitude"), Bytes.toBytes(location(1)))

                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("formatted_address"), Bytes.toBytes(locationJson.getString("formatted_address")))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("province"), Bytes.toBytes(locationJson.getString("province")))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("city"), Bytes.toBytes(locationJson.getString("city")))
                    if (!locationJson.isNull("citycode")) put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("citycode"), Bytes.toBytes(locationJson.getString("citycode")))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("district"), Bytes.toBytes(locationJson.getString("district")))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("adcode"), Bytes.toBytes(locationJson.getString("adcode")))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("level"), Bytes.toBytes(locationJson.getString("level")))
                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("json"), Bytes.toBytes(sb.toString()))

                    table.put(put)
                  } else {
                    val put = new Put(Bytes.toBytes(address))
                    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("order_no"), Bytes.toBytes(orderNo))
                    put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(sb.toString()))
                    table2.put(put)
                    logger.info("no coordinate")
                  }
                case "10001" =>
                  val put = new Put(Bytes.toBytes(address))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("order_no"), Bytes.toBytes(orderNo))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(sb.toString()))
                  table2.put(put)
                  logger.error("key不正确或过期")
                  throw new Exception("key不正确或过期")
                case "10002" =>
                  val put = new Put(Bytes.toBytes(address))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("order_no"), Bytes.toBytes(orderNo))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(sb.toString()))
                  table2.put(put)
                  logger.error("没有权限使用相应的服务或者请求接口的路径拼写错误")
                  throw new Exception("没有权限使用相应的服务或者请求接口的路径拼写错误")
                case "10003" =>
                  val put = new Put(Bytes.toBytes(address))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("order_no"), Bytes.toBytes(orderNo))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(sb.toString()))
                  table2.put(put)
                  logger.error("访问已超出日访问量")
                  throw new Exception("访问已超出日访问量")
                case e =>
                  val put = new Put(Bytes.toBytes(address))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("order_no"), Bytes.toBytes(orderNo))
                  put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(sb.toString()))
                  table2.put(put)
                  logger.error("error info code: " + json)
                  throw new Exception("error info code: " + e)
              }
            case e =>
              val put = new Put(Bytes.toBytes(address))
              put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("order_no"), Bytes.toBytes(orderNo))
              put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("result"), Bytes.toBytes(sb.toString()))
              table2.put(put)
              logger.error("not known status: " + e)
              throw new Exception("don't know status:" + e)
          }
        }

      }
    }
//
//    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
//    import spark.implicits._
//    import spark.sql
//
//    sql(s).foreachPartition { partition =>
//      val baseUrl = "http://restapi.amap.com/v3/geocode/geo"
//      val key = "47ceb1d05c0e8d7e20e5fdcf90f83246"
//      val conf = HBaseConfiguration.create()
//      conf.set("hbase.zookeeper.quorum", "m79sit,s80sit,s81sit")
//      val hBaseConn = ConnectionFactory.createConnection(conf)
//      val table = hBaseConn.getTable(TableName.valueOf("addressCoordinate"))
//
//      partition.foreach { case Row(address: String) =>
//        // 当地址有效，并且hbase中不存在这条记录，则去查找，否则不查找
//        if (address != null && address.length > 0 && !table.exists(new Get(Bytes.toBytes(address)))) {
//          val url = s"$baseUrl?key=$key&address=$address".replace(" ", "")
//          logger.debug(url)
//          println(url)
//          val u = getConnection(url, 3)
//          val buffer = new BufferedReader(new java.io.InputStreamReader(u.openConnection.getInputStream))
//          val sb = new StringBuilder
//          var reader = buffer.readLine()
//          while (reader != null) {
//            sb.append(reader)
//            reader = buffer.readLine()
//          }
//          val json = new JSONObject(sb.toString())
//          json.getString("status") match {
//            case "0" =>
//              logger.error("request failed, failed info: " + json.getString("info"))
//            case "1" =>
//              json.getString("info") match {
//                case "10000" => val count = json.getString("count").toInt
//                  if (count != 0) {
//                    logger.info("more than on coordinates: " + sb.toString())
//                  } else {
//                    val location = json.getJSONArray("geocodes").getJSONObject(0).getString("location").split(",")
//                    val put = new Put(Bytes.toBytes(address))
//                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("longitude"), Bytes.toBytes(location(0)))
//                    put.addColumn(Bytes.toBytes("coordinate"), Bytes.toBytes("latitude"), Bytes.toBytes(location(1)))
//                    table.put(put)
//                  }
//                case "10001" =>
//                  logger.error("key不正确或过期")
//                  throw new Exception("key不正确或过期")
//                case "10002" =>
//                  logger.error("没有权限使用相应的服务或者请求接口的路径拼写错误")
//                  throw new Exception("没有权限使用相应的服务或者请求接口的路径拼写错误")
//                case "10003" =>
//                  logger.error("访问已超出日访问量")
//                  throw new Exception("访问已超出日访问量")
//                case e =>
//                  logger.error("error info code: " + e)
//                  throw new Exception("error info code: " + e)
//              }
//            case e =>
//              logger.error("not known status: " + e)
//              throw new Exception("don't know status:" + e)
//          }
//        }
//      }
//    }

  }

  def getConnection(url: String, time: Int): BufferedReader = {
    if (time <= 0) throw new Exception("断开重连超过三次")
    else {
      try {
        val u = new URL(url)
        new BufferedReader(new java.io.InputStreamReader(u.openConnection.getInputStream))
      } catch {
        case e: Throwable =>
          Thread.sleep(1000)
          getConnection(url, time - 1)
      }
    }
  }

}
