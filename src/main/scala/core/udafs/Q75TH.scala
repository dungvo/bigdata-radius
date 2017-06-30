package core.udafs

/**
  * Created by hungdv on 30/06/2017.
  */
class Q75TH extends org.apache.spark.sql.expressions.UserDefinedAggregateFunction{
  def inputSchema: org.apache.spark.sql.types.StructType =
    org.apache.spark.sql.types.StructType(org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.DoubleType) :: Nil)

  def bufferSchema: org.apache.spark.sql.types.StructType = org.apache.spark.sql.types.StructType(
    org.apache.spark.sql.types.StructField("window_list", org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.DoubleType, false)) :: Nil
  )


  def dataType: org.apache.spark.sql.types.DataType = org.apache.spark.sql.types.DoubleType

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
    var q75Th = computePercentile(sortedWindow,75,false)
    q75Th
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

}
