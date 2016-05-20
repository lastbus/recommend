package com.bl.bigdata.accumulator

import org.apache.spark.{AccumulatorParam, Accumulable}

/**
 * Created by MK33 on 2016/5/20.
 */
class ErrorMessageAccumulator[T](@transient initialValue: T, param: AccumulatorParam[T], name: Option[String])
  extends Accumulable[T, T](initialValue, param, name) {


}
