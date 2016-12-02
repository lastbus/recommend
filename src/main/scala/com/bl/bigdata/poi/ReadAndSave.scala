package com.bl.bigdata.poi

import java.io.{BufferedReader, FileInputStream, FileOutputStream}
import java.net.URL

import com.bl.bigdata.recommend.OrderAddressToHBase
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.json.JSONObject

/**
  * Created by MK33 on 2016/10/19.
  */
object ReadAndSave {

  def main(args: Array[String]) {

//    val file = "D:\\订单区域分析\\tmp.xlsx"
    val file = "C:\\Users\\mk33\\Desktop\\订单地域分析.xlsx"
    val baseUrl = "http://restapi.amap.com/v3/geocode/geo"
    val key = "02ea79be41a433373fc8708a00a2c0fa"

    val inputStream = new FileInputStream(file)
    val wb = new XSSFWorkbook(inputStream)
    val sheet = wb.getSheetAt(0)
    val row = sheet.getRow(2)
    val rowIterator = sheet.rowIterator()
    rowIterator.next()
    rowIterator.next()
    while (rowIterator.hasNext) {
      val cell = rowIterator.next()
      val address = "上海市" + cell.getCell(2).getStringCellValue
      println(address)
      val url = s"$baseUrl?key=$key&address=$address"
      val buffer = getConnection(url, 3)
      val sb = new StringBuilder
      var reader = buffer.readLine()
      while (reader != null) {
        sb.append(reader)
        reader = buffer.readLine()
      }
      println(sb.toString())
      val json = new JSONObject(sb.toString())
      try {
        val locationJson = json.getJSONArray("geocodes").getJSONObject(0)
        val district  = locationJson.getString("district")
        cell.getCell(0).setCellValue(district)
      } catch {
        case e: Exception =>
          println("error " + address)
          e.printStackTrace()
      }


    }
    inputStream.close()
    val out = new FileOutputStream(file)
    wb.write(out)

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
