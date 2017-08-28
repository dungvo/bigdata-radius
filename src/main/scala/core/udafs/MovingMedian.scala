package core.udafs

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable

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



// Research purpose
class String_AGG extends org.apache.spark.sql.expressions.UserDefinedAggregateFunction {
  def inputSchema: org.apache.spark.sql.types.StructType =
    org.apache.spark.sql.types.StructType(org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.StringType) :: Nil)

  def bufferSchema: org.apache.spark.sql.types.StructType = org.apache.spark.sql.types.StructType(
    org.apache.spark.sql.types.StructField("window_list", org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.StringType, false)) :: Nil
  )

  def dataType: org.apache.spark.sql.types.DataType = org.apache.spark.sql.types.StringType

  def deterministic: Boolean = true

  def initialize(buffer: org.apache.spark.sql.expressions.MutableAggregationBuffer): Unit = {
    buffer(0) = new scala.collection.mutable.ArrayBuffer[String]()
  }

  def update(buffer: org.apache.spark.sql.expressions.MutableAggregationBuffer,input: org.apache.spark.sql.Row): Unit = {
    var bufferVal = buffer.getAs[scala.collection.mutable.WrappedArray[String]](0).toBuffer
    bufferVal += input.getAs[String](0)
    buffer(0) = bufferVal
  }

  def merge(buffer1: org.apache.spark.sql.expressions.MutableAggregationBuffer, buffer2: org.apache.spark.sql.Row): Unit = {
    buffer1(0) = buffer1.getAs[scala.collection.mutable.ArrayBuffer[String]](0) ++ buffer2.getAs[scala.collection.mutable.ArrayBuffer[String]](0)
  }

  def evaluate(buffer: org.apache.spark.sql.Row): Any = {
    var stringsSeq = buffer.getAs[scala.collection.mutable.WrappedArray[String]](0).toBuffer
    stringsSeq.mkString(",")

  }

}

// Research purpose.
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class GroupByStringAgg extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    org.apache.spark.sql.types.StructType(org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.StringType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = org.apache.spark.sql.types.StructType(
    org.apache.spark.sql.types.StructField("window_list", org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.StringType, false)) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  def initialize(buffer: org.apache.spark.sql.expressions.MutableAggregationBuffer): Unit = {
    buffer(0) = new scala.collection.mutable.ArrayBuffer[String]()
  }

  def update(buffer: org.apache.spark.sql.expressions.MutableAggregationBuffer,input: org.apache.spark.sql.Row): Unit = {
    //var bufferVal = buffer.getAs[scala.collection.mutable.ArrayBuffer[String]](0)
    var bufferVal = buffer.getAs[scala.collection.mutable.WrappedArray[String]](0).toBuffer
    bufferVal += input.getAs[String](0)
    buffer(0) = bufferVal
  }

  def merge(buffer1: org.apache.spark.sql.expressions.MutableAggregationBuffer, buffer2: org.apache.spark.sql.Row): Unit = {
    val a = buffer1.getAs[scala.collection.mutable.WrappedArray[String]](0)
    val b = buffer2.getAs[scala.collection.mutable.ArrayBuffer[String]](0)
    buffer1(0) = a ++ b
    //buffer1(0) = buffer1.getAs[scala.collection.mutable.ArrayBuffer[String]](0) ++ buffer2.getAs[scala.collection.mutable.ArrayBuffer[String]](0)
  }

  def evaluate(buffer: org.apache.spark.sql.Row): Any = {
    var stringsSeq = buffer.getAs[scala.collection.mutable.WrappedArray[String]](0)
    stringsSeq.mkString(",")

  }
}

