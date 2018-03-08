package com.ftel.bigdata.radius.stats

import com.ftel.bigdata.conf.Configure
import org.apache.spark.sql.SparkSession
import com.ftel.bigdata.utils.DateTimeUtil
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import com.ftel.bigdata.utils.HdfsUtil

object Feature {
  def main(args: Array[String]) {
    val sample1 = Array(
        Difference("A", 10, 20, 30, 40),
        Difference("B", 10, 20, 30, 40),
        Difference("C", 10, 20, 30, 40)
    )
    
    val sample2 = Array(
        Difference("A", 100, 200, 300, 400),
        Difference("C", 100, 200, 300, 400),
        Difference("E", 100, 200, 300, 400)
    )
    
    val sample3 = Array(
        Difference("A", 1000, 2000, 3000, 4000),
        Difference("B", 1000, 2000, 3000, 4000),
        Difference("D", 1000, 2000, 3000, 4000)
    )
    
    val sc = Configure.getSparkContextLocal()
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    //val df = loadStats.toDF
    
    val df1 = sc.parallelize(sample1, 1).toDF("name", "d1", "u1", "dd1", "ud1")//.map(x => x.toPair)
    val df2 = sc.parallelize(sample2, 1).toDF("name", "d2", "u2", "dd2", "ud2")//.map(x => x.toPair)
    val df3 = sc.parallelize(sample3, 1).toDF("name", "d3", "u3", "dd3", "ud3")//.toDF//.map(x => x.toPair)
    
    val arr = Array(df1, df2, df3)
    //val rdd = sc.parallelize(sample1 ++ sample2, 1).toDF
    //rdd1.join(rdd2).join(rdd3).foreach(println)
    
    //df1.join(df2, 'name).show()
    
    //rdd.show()
    
    //df1.join(df2).toDF.show
    
//    df1.join
    //df1.join(df2, $"name" === $"name")
    //df1.join(df2, df1.col("name") === df2.col("name"), "full").drop(df2.col("name")).show
    //df1.join(df3)
//    df1.join(df2, df1.col("name") === df2.col("name"), "full").drop(df2.col("name"))
//       .join(df3, df1.col("name") === df3.col("name"), "full").drop(df3.col("name"))
//       .show
    
    //arr.reduce((x,y) => x.join(y, x.col("name") === y.col("name"), "full").drop(x.col("name"))).show
    join(arr).show
  }
  
  
  def save(sparkSession: SparkSession, month: String) {
    val dateTime = DateTimeUtil.create(month + "-01", DateTimeUtil.YMD).dayOfMonth().withMinimumValue()
    val number = dateTime.dayOfMonth().getMaximumValue()
    def merge(df: Array[Dataset[Row]], index: Int): Array[Dataset[Row]] = {
      if (index < number) {
        val dfTemp = read(sparkSession, dateTime.plusDays(index).toString(DateTimeUtil.YMD))
        merge(df :+ dfTemp, index + 1)
        //merge(df.join(dfTemp, df.col("name") === dfTemp.col("name"), "full").drop(dfTemp.col("name")), index+1)
      } else df
    }
    val indexBegin = 0
    
    //val session = Session.calFor28DaysInMonth(sparkSession, month)
    val df = merge(Array(read(sparkSession, dateTime.plusDays(indexBegin).toString(DateTimeUtil.YMD))), indexBegin+1)// :+ session
    join(df).write
      .option("header", "true")
      .csv(s"/data/radius/feature/multiply-files/${month}")
    
//    df.reduce((x,y) => x.join(y, x.col("name") === y.col("name")).drop(x.col("name")))
//      //.coalesce(32)
//      .write
//      .option("header", "true")
//      .csv(s"/data/radius/feature/multiply-files/${month}")
      
    mergeFeatureFiles(sparkSession, month)
  }
  
  def mergeFeatureFiles(sparkSession: SparkSession, month: String) {
    val df = sparkSession.read.option("header", "true").csv(s"/data/radius/feature/multiply-files/${month}")
    val session = Session.calForOneMonth(sparkSession, month)
    df.join(session, df.col("name") === session.col("name"))
      .drop(session.col("name"))
      .write.option("header", "true").csv(s"/data/radius/feature/${month}-temp")
    val df2 = sparkSession.read.option("header", "true").csv(s"/data/radius/feature/${month}-temp")
    df2.coalesce(1).write.option("header", "true").csv(s"/data/radius/feature/${month}")
  }
  
  private def join(arr: Array[Dataset[Row]]) = {
    //arr.reduce((x,y) => x.join(y, x.col("name") === y.col("name"), "full").drop(y.col("name")))//.show
    arr.reduce((x,y) => x.join(y, Seq("name"), "full"))//.show
  }

//  private def calAttend() {
//    
//  }

//  def calSessionOneMonth(sparkSession: SparkSession, month: String) {
//    val dateTime = DateTimeUtil.create(month + "-01", DateTimeUtil.YMD).dayOfMonth().withMinimumValue()
//    val number = dateTime.dayOfMonth().getMaximumValue()
//    def merge(df: Array[Dataset[Row]], index: Int): Array[Dataset[Row]] = {
//      if (index < number) {
//        val dfTemp = read(sparkSession, dateTime.plusDays(index).toString(DateTimeUtil.YMD))
//        merge(df :+ dfTemp, index + 1)
//        //merge(df.join(dfTemp, df.col("name") === dfTemp.col("name"), "full").drop(dfTemp.col("name")), index+1)
//      } else df
//    }
//    val indexBegin = 0
//    val df = merge(Array(read(sparkSession, dateTime.plusDays(indexBegin).toString(DateTimeUtil.YMD))), indexBegin+1)
//    df.reduce((x,y) => x.join(y, x.col("name") === y.col("name")).drop(x.col("name")))
//      .coalesce(32)
//      .write
//      .option("header", "true")
//      .csv(s"/data/radius/feature/${month}")
//  }
  
  private def read(sparkSession: SparkSession, day: String) = {
    val number = DateTimeUtil.create(day, "yyyy-MM-dd").getDayOfMonth()
    import sparkSession.implicits._
    sparkSession.sparkContext
      .textFile(s"/data/radius/stats/${day}/diff", 1)
      .map(x => x.split("\t"))
      .map(x => new Difference(x))
      .toDF("name", s"Download${number}Size", s"Upload${number}Size", s"Download${number}Diff", s"Upload${number}Diff")
  }
}