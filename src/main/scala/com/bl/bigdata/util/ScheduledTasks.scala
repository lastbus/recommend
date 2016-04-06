//package com.bl.bigdata.util
//
//import java.text.SimpleDateFormat
//import java.util.Date
//
//import org.springframework.scheduling.annotation.Scheduled
//import org.springframework.stereotype.Component
//
///**
//  * Created by MK33 on 2016/3/29.
//  */
//@Component
//class ScheduledTasks {
//
//  private val dateFormat = new SimpleDateFormat("HH:mm:ss")
//  @Scheduled(fixedRate = 5000)
//  def reportCurrentTime() {
//    System.out.println("The time is now " + dateFormat.format(new Date()))
//  }
//
//}
