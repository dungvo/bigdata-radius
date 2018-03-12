package com.ftel.bigdata.radius.stats

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.ftel.bigdata.radius.utils.BrasUtil

object Functions {
  
  @deprecated
  def calculateLoad(sparkSession: SparkSession, loadStats: RDD[LoadStats], maxValue: Long, threshold: Long): RDD[LoadStats] = {
    import sparkSession.implicits._
    val df = loadStats.toDF
    val byNameAndSessionIDWithtimeStampAsc = Window.partitionBy('name, 'sessionId).orderBy('timestamp asc)
    val downloadCurrent = 'download
    val downloadPrevious = lag('download, 1).over(byNameAndSessionIDWithtimeStampAsc)
    
    val donwloadRef = 
      when(downloadPrevious.isNull, downloadCurrent) otherwise (  // If current is first line
        when(downloadCurrent >= downloadPrevious, downloadCurrent - downloadPrevious) otherwise (  // if current > previous => load = current - previous
          when(downloadPrevious > (maxValue - threshold), downloadCurrent + maxValue - downloadPrevious) otherwise downloadCurrent // if maxValue - previous < threshold => current + maxValue - previous else current
        )
      )
    
    val uploadCurrent = 'upload
    val uploadPrevious = lag('upload, 1).over(byNameAndSessionIDWithtimeStampAsc)
    
    val uploadRef = 
      when(uploadPrevious.isNull, uploadCurrent) otherwise (  // If current is first line
        when(uploadCurrent >= uploadPrevious, uploadCurrent - uploadPrevious) otherwise (  // if current > previous => load = current - previous
          when(uploadPrevious > (maxValue - threshold), uploadCurrent + maxValue - uploadPrevious) otherwise uploadCurrent // if maxValue - previous < threshold => current + maxValue - previous else current
        )
      )
    
    val results = df.select('timestamp, 'name, 'sessionId, 'sessionTime, 'download, 'upload, donwloadRef as 'dRef, uploadRef as 'uRef)
    results.rdd.map(x => LoadStats(
        x.getLong(0),
        x.getString(1),
        x.getString(2),
        x.getLong(3),
        x.getLong(6),
        x.getLong(7)//,
        //x.getLong(6),
        //x.getLong(7)
        ))
  }
  
  def calculateLoad(sparkSession: SparkSession, loadStats: RDD[LoadStats]): RDD[LoadStats] = {
    val res = loadStats.filter(x => x.download >= 0 && x.upload >= 0)
             .map(x => (x.name + x.sessionId) -> x)
             .reduceByKey(getLogLastByTime)
             .map(x => x._2)
    //res.foreach(println)
    res
    //null
  }
  
  private def getLogLastByTime = (x: LoadStats, y: LoadStats) => {
    if (x.timestamp > y.timestamp) x else y
  }
  
  def parseTuple(line: String): (String, (Long, Long)) = {
    //(sgdsl-130607-981,(5877228473,252995046))
    val arr = line.trim().substring(1, line.trim().length()-1).split(",")
    arr(0) -> (arr(1).substring(1).toLong, arr(2).substring(0, arr(2).length()-1).toLong)
  }
}