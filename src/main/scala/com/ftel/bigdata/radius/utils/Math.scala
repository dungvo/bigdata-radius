package com.ftel.bigdata.radius.utils

import com.ftel.bigdata.conf.Configure
import org.apache.spark.sql.SparkSession


object Math {
  def quantile(data: Array[Int], quantileNum: Int) = {
//    val spark = SparkSession.builder().getOrCreate()
//    import spark.implicits._
//
//    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("value")
//
//    df.createOrReplaceTempView("table")
//    spark.sql(s"SELECT PERCENTILE(value, ${quantileNum/100.0}) FROM table").collect()(0).getDouble(0).toInt

    data.sortWith(_ < _)((quantileNum/100.0 * data.length).toInt-1)
  }

  def quantile(data: Array[Double], quantileNum: Int) = {

    data.sortWith(_ < _)((quantileNum/100.0 * data.length).toInt-1)
  }

  def quantile(data: Array[Long], quantileNum: Int) = {

    data.sortWith(_ < _)((quantileNum/100.0 * data.length).toInt)
  }

  def quantile(data: Iterable[(Long, Long)], quantileNum: Int): (Long, Long) = {

    val sl = data.map(_._1).toArray
    val ls = data.map(_._2).toArray
    (quantile(sl, quantileNum), quantile(ls, quantileNum))
  }

  def IQR(data: Iterable[(Long, Long)]): (Long, Long) = {
    val sl = data.map(_._1).toArray
    val ls = data.map(_._2).toArray
    (quantile(sl, 75) + 3*(quantile(sl, 75)- quantile(sl, 25)), quantile(ls, 75) + 3*(quantile(ls, 75)- quantile(ls, 25)))
  }

  def main(args: Array[String]): Unit = {
    val sc = Configure.getSparkContextLocal
    val data = Array(1,1,1,1,1,2,2,2,3,4,5,39)

    println(quantile(data, 25))
    println(quantile(data, 50))
    println(quantile(data, 75))
    println(quantile(data, 95))

  }
}
