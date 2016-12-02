package com.bl.bigdata.poi

import java.io._

import org.apache.poi.xssf.usermodel.XSSFWorkbook

/**
  * Created by MK33 on 2016/10/17.
  */
object TransformTextToExcel {

  def main(args: Array[String]) {

    val in = new BufferedReader(new InputStreamReader(new FileInputStream("F:\\download\\chrome\\part-00000 (20)")))
    var line = in.readLine()
    val wb = new XSSFWorkbook()
    val sheet = wb.createSheet("销售金额")
    val sheet2 = wb.createSheet("销售数量")

    val header: Array[String] = new Array[String](19)
    header(0) = "店名"
    header(1) = "地址"
    var conflict = false
//    val createHelper = wb.getCreationHelper
    // i: excel line number
    var i = 0
    while (line != null) {
      line = line.replace("(", "").replace(")", "")
      var row = sheet.createRow(i)
      var row2 = sheet2.createRow(i)
      val cells = line.split(",")
      // extract excel header
      if (i == 0) {
        for (m <- 2 until cells.length) {
//          println(m)
          val s = cells(m).split("#")
          header(m) = s(1) + "(" +  s(0) + ")"
//          println(header(m))
        }
        for (m <- 0 until header.length) {
          row.createCell(m).setCellValue(header(m))
          row2.createCell(m).setCellValue(header(m))
        }
        i += 1
        row = sheet.createRow(i)
        row2 = sheet2.createRow(i)
      }

      // cell content
      for (j  <- 0 until cells.length) {

        if (j > 1) {
          // category
          val t = cells(j).split("#")
          if (s"${t(1)}(${t(0)})" != header(j)) {
            println( s"${t(1)}(${t(0)}) -------- ${header(j)}")
            conflict = true
          }
          if (cells(j).contains("not-a-value")) {
            row.createCell(j).setCellValue("0.0")
            row2.createCell(j).setCellValue("0.0")
          } else {
            row.createCell(j).setCellValue(t(2))
            row2.createCell(j).setCellValue(t(3))
          }

        } else {
          // store name and location
          row.createCell(j).setCellValue(cells(j))
          row2.createCell(j).setCellValue(cells(j))
        }

      }

      i += 1
      line = in.readLine()
    }

    if (i > 0) {
      val out = new FileOutputStream("D:\\订单区域分析\\tmp.xlsx")
      wb.write(out)
      out.close()
    }

    if (conflict) {
      throw new Exception("conflict header")
    }



  }

}
