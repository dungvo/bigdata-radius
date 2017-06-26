package core.udafs

import org.apache.spark.sql.types.{DoubleType, StructType}

/**
  * Created by hungdv on 11/06/2017.
  */
class BounderIQR extends org.apache.spark.sql.expressions.UserDefinedAggregateFunction {
  def inputSchema: org.apache.spark.sql.types.StructType =
    org.apache.spark.sql.types.StructType(org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.DoubleType) :: Nil)

  def bufferSchema: org.apache.spark.sql.types.StructType = org.apache.spark.sql.types.StructType(
    org.apache.spark.sql.types.StructField("window_list", org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.DoubleType, false)) :: Nil
  )

  def dataType: StructType = new StructType()
    .add("upper",DoubleType)
    .add("below",DoubleType)


  //def dataType: org.apache.spark.sql.types.DataType = org.apache.spark.sql.types.DoubleType

  def deterministic: Boolean = true

  def initialize(buffer: org.apache.spark.sql.expressions.MutableAggregationBuffer): Unit = {
    buffer(0) = new scala.collection.mutable.ArrayBuffer[Double]()
  }

  def update(buffer: org.apache.spark.sql.expressions.MutableAggregationBuffer,input: org.apache.spark.sql.Row): Unit = {
    var bufferVal = buffer.getAs[scala.collection.mutable.WrappedArray[Double]](0).toBuffer
    bufferVal += input.getAs[Double](0)
    buffer(0) = bufferVal
  }

  def merge(buffer1: org.apache.spark.sql.expressions.MutableAggregationBuffer, buffer2: org.apache.spark.sql.Row): Unit = {
    buffer1(0) = buffer1.getAs[scala.collection.mutable.ArrayBuffer[Double]](0) ++ buffer2.getAs[scala.collection.mutable.ArrayBuffer[Double]](0)
  }

  def evaluate(buffer: org.apache.spark.sql.Row): Any = {
    var sortedWindow = buffer.getAs[scala.collection.mutable.WrappedArray[Double]](0).sorted.toBuffer
    var windowSize = sortedWindow.size
    var q25Th = computePercentile(sortedWindow,25,false)
    var q75Th = computePercentile(sortedWindow,75,false)

    val IQR = q75Th - q25Th
    val min = q25Th - 3*IQR
    //val min = q25Th - 1.5*IQR
    val max = q75Th + 3*IQR
    //val max = q75Th + 1.5*IQR
    (max,min)

  }

  //
  def computePercentile(vals: Seq[Double], tile: Double, unsorted: Boolean = true): Double = {
    assert(tile >= 0 && tile <= 100)
    if (vals.isEmpty) Double.NaN
    else {
      assert(vals.nonEmpty)
      // Make sure the list is sorted, if that's what we've been told
      if (!unsorted && vals.length >= 2) vals.sliding(2).foreach(l => assert(l(0) <= l(1))) else {}
      // NIST method; data to be sorted in ascending order
      val r =
        if (unsorted) vals.sorted
        else vals
      val length = r.length
      if (length == 1) r.head
      else {
        val n = (tile / 100d) * (length - 1)
        val k = math.floor(n).toInt
        val d = n - k
        if (k <= 0) r.head
        else {
          val last = length
          if (k + 1 >= length) {
            r.last
          } else {
            r(k) + d * (r(k + 1) - r(k))
          }
        }
      }
    }
  }
  //inaccuracy
  /*def computePercentile2(vals: Seq[Double], tile: Double, unsorted: Boolean = true): Double = {
    assert(tile >= 0 && tile <= 100)
    if (vals.isEmpty) Double.NaN
    else {
      assert(vals.nonEmpty)
      // Make sure the list is sorted, if that's what we've been told
      if (!unsorted && vals.length >= 2) vals.sliding(2).foreach(l => assert(l(0) <= l(1))) else {}
      // NIST method; data to be sorted in ascending order
      val r =
        if (unsorted) vals.sorted
        else vals
      val length = r.length
      if (length == 1) r.head
      else {
        val n = (tile / 100d) * (length + 1d)
        val k = math.floor(n).toInt
        val d = n - k
        if (k <= 0) r.head
        else {
          val last = length
          if (k  >= length) {
            r(length-1)
          } else {
            r(k-1) + d * (r(k) - r(k-1))
          }
        }
      }
    }
  }*/


}