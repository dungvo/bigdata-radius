package core.udafs

/**
  * Created by hungdv on 11/06/2017.
  */
class MovingMedian extends org.apache.spark.sql.expressions.UserDefinedAggregateFunction {
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
    if(windowSize%2==0){
      var index=windowSize/2
      (sortedWindow(index) + sortedWindow(index-1))/2
    }else{
      var index = (windowSize+1)/2 - 1
      sortedWindow(index)
    }
  }

}
