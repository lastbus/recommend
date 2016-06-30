package com.bl.bigdata.streaming

/**
 * Created by MK33 on 2016/6/29.
 */
object TT {

  def main(args: Array[String]) {

//    HotSaleCommandParser.parser(args)

    while(true){
      new Thread(new Runnable(){
        def run() {
          try {
            Thread.sleep(10000000);
          } catch {
            case e: Exception =>println(e.getMessage)
          }
        }
      }).start()
    }



  }
}
