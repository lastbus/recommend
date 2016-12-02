package actor


/**
  * Created by MK33 on 2016/11/10.
  */
object Test {

  def main(args: Array[String]) {
    val recReg = "/recommend/rec.*".r
    val rec = "/recommend/rec?api=cart&memberId=100000001783152&items=349962,215749,120883,354582,166370,167137,182775,232797,311428,254288,74805,303222,229330,69762,220807,298538,298577&chan=3&pNum=1&pSize=16"
    println(recReg.findFirstMatchIn(rec).isDefined)

    val badReg = "/recommend/bab?mId=100000000796281&chan=1&gId=316952".r
    val bad = "/recommend/bab?mId=100000000796281&chan=1&gId=316952"

  }

}