package com.bl.bigdata.tfidf

import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.junit._

/**
  * Created by MK33 on 2016/3/17.
  */
@Test
class MainTest {

  val attr = Array("袋装", "中国", "零食", "水果", "坚果")
  val priceList: List[Double] = List(0, 23.0, 45.9, 100.0, 500)

  @Test
  def testCalculatorTF() = {
    val m = new GoodsSimilarityInCate
    val feature = m.featuresNum
    val tf = m.calculatorTF(attr, "500", priceList)
    tf match {
      case SparseVector(size, indices, values) =>
        assert(size == feature + 1)
        val nnz = indices.length
        assert(nnz == 6)
        var k = 0
        while (k < nnz - 1){
          assert(tf(indices(k)) == 1.0)
          k += 1
        }
      case other => assert(false)
    }
    assert(m.featuresNum + 1 == tf.size)
    assert(5 == tf(feature))
    val tf2 = m.calculatorTF(attr, "20", priceList)
    assert(1 == tf2(feature))

    val tf3 = m.calculatorTF(attr, "null", priceList)
    assert(3.0 == tf3(feature))
  }


}
