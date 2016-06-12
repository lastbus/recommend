package com.bl.bigdata.accumulator

import org.apache.spark.AccumulatorParam

/**
 * Created by MK33 on 2016/5/20.
 */
object MyAccumulator {

  implicit object StringAccumulatorParam extends AccumulatorParam[StringBuffer] {
    def addInPlace(t1: Float, t2: Float) = t1 + t2
    def zero(initialValue: Float) = new StringBuffer()
    override def addInPlace(r1: StringBuffer, r2: StringBuffer): StringBuffer = r1.append(r2)

    override def zero(initialValue: StringBuffer): StringBuffer = new StringBuffer()
  }

}



