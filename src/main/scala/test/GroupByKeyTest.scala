package test

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

/**
  * Created by hungdv on 22/08/2017.
  */
object GroupByKeyTest {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("gt").master("local[*]").getOrCreate()
    val seq = Seq(("e1","2017-08-21 23:21:47.0"),
      ("e2","2017-08-21 23:23:47.0"),
      ("e1","2017-08-21 23:25:47.0"),
      ("e2","2017-08-21 23:29:47.0"))
    val rdd = ss.sparkContext.parallelize(seq)
    val groupedByKey = rdd.map{
      pair => (pair._1,stringToLong(pair._2))
    }
      .groupByKey()
    groupedByKey.foreach{pair =>
      var value = pair._2.toList
      println(pair._1 + " value size " + value.min + " min time " + value.sorted + "original time " )
    }
  }
  def stringToLong(s: String): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val ms = sdf.parse(s).getTime()
    ms
  }



}
