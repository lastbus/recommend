package com.bl.bigdata.tfidf


import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}


/**
  * Created by MK33 on 2016/3/10.
  */

/** Document frequency aggregator. */
class DocumentFrequencyAggregator(val minDocFreq: Int) extends Serializable {

  /** number of documents */
  private var m = 0L
  /** document frequency vector */
  private var df: BDV[Long] = _


  def this() = this(0)

  /** Adds a new document. */
  def add(doc: Vector): this.type = {
    if (isEmpty) {
      df = BDV.zeros(doc.size)
    }
    doc match {
      case SparseVector(size, indices, values) =>
        val nnz = indices.size
        var k = 0
        while (k < nnz) {
          if (values(k) > 0) {
            df(indices(k)) += 1L
          }
          k += 1
        }
      case DenseVector(values) =>
        val n = values.size
        var j = 0
        while (j < n) {
          if (values(j) > 0.0) {
            df(j) += 1L
          }
          j += 1
        }
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
    m += 1L
    this
  }

  /** Merges another. */
  def merge(other: DocumentFrequencyAggregator): this.type = {
    if (!other.isEmpty) {
      m += other.m
      if (df == null) {
        df = other.df.copy
      } else {
        df += other.df
      }
    }
    this
  }

  private def isEmpty: Boolean = m == 0L

  /** Returns the current IDF vector. */
  def idf(): Vector = {
    if (isEmpty) {
      throw new IllegalStateException("Haven't seen any document yet.")
    }
    val n = df.length
    val inv = new Array[Double](n)
    var j = 0
    while (j < n) {
      /*
       * If the term is not present in the minimum
       * number of documents, set IDF to 0. This
       * will cause multiplication in IDFModel to
       * set TF-IDF to 0.
       *
       * Since arrays are initialized to 0 by default,
       * we just omit changing those entries.
       */
      if(df(j) >= minDocFreq) {
        inv(j) = math.log((m + 1.0) / (df(j) + 1.0))
      }
      j += 1
    }
    Vectors.dense(inv)
  }

}

object IDFModel {

  /**
    * Transforms a term frequency (TF) vector to a TF-IDF vector with a IDF vector
    *
    * @param idf an IDF vector
    * @param v a term frequence vector
    * @return a TF-IDF vector
    */
  def transform(idf: Vector, v: Vector): Vector = {
    val n = v.size
    v match {
      case SparseVector(size, indices, values) =>
        val nnz = indices.size
        val newValues = new Array[Double](nnz)
        var k = 0
        while (k < nnz) {
          newValues(k) = values(k) * idf(indices(k))
          k += 1
        }
        Vectors.sparse(n, indices, newValues)
      case DenseVector(values) =>
        val newValues = new Array[Double](n)
        var j = 0
        while (j < n) {
          newValues(j) = values(j) * idf(j)
          j += 1
        }
        Vectors.dense(newValues)
      case other =>
        throw new UnsupportedOperationException(
          s"Only sparse and dense vectors are supported but got ${other.getClass}.")
    }
  }
}

