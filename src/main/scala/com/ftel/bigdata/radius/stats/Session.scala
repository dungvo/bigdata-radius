package com.ftel.bigdata.radius.stats

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.ftel.bigdata.radius.classify.LoadLog
import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.utils.Parameters
import org.apache.spark.storage.StorageLevel
import com.ftel.bigdata.radius.RadiusParameters

case class Session(day: String, name: String, id: String, time: Long) {
  private def this(arr: Array[String]) = this(arr(0), arr(1), arr(2), arr(3).toLong)
  def this(line: String) = this(line.split("\t"))
  override def toString = Array(day, name, id, time).mkString("\t")
}

object Session {
  def main(args: Array[String]) {

  }

  
  def save(rdd: RDD[LoadStats], day: String) {
    rdd.map(x => (x.name.toLowerCase(), x.sessionId.toLowerCase()) -> x.sessionTime)
      .reduceByKey(Math.max)
      .map(x => Session(day, x._1._1, x._1._2, x._2))
      .coalesce(32, false, None)
      .saveAsTextFile(RadiusParameters.STATS_PATH + s"/${day}/session")
  }
  
  def calForOneMonth(sparkSession: SparkSession, month: String) = {
    val path = RadiusParameters.STATS_PATH + s"/${month}-*/session"
    val session = sparkSession.sparkContext.textFile(path, 1)
      .map(x => new Session(x))
      // Có 1 vài dòng log với sessionTime lơn hơn 1 ngày, sẽ gán lại sessionTime của nó là max 1 ngày
      .map(x => if (x.time > 86400L) Session(x.day, x.name, x.id, 86400L) else x)
      .filter(x => x.time > 0)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
      //.filter(x => DateTimeUtil.create(x.day, DateTimeUtil.YMD).getDayOfMonth() <= 28)

    val attendVal = session.map(x => x.name -> x.day)
      .distinct()
      .map(x => x._1 -> 1)
      .reduceByKey(_+_)
      
    val sessionSumTime = session
      .map(x => (x.name + x.id) -> x)
      .reduceByKey((x,y) => Session(x.day, y.name, y.id, x.time + y.time))
      .map(x => x._2)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
      
    val countVal = sessionSumTime.map(x => x.name -> x.id).distinct().map(x => x._1 -> 1).reduceByKey(_+_)
    val minVal = sessionSumTime.map(x => x.name -> x.time).reduceByKey(Math.min)
    val maxVal = sessionSumTime.map(x => x.name -> x.time).reduceByKey(Math.max)
    val meanVal = sessionSumTime.map(x => x.name -> x.time.toDouble).groupByKey().map(x => x._1 -> mean(x._2))
    val stdVal = sessionSumTime.map(x => x.name -> x.time.toDouble).groupByKey().map(x => x._1 -> std(x._2))
    
//    val pathLoad = RadiusParameters.CLASSIFY_PATH + s"/day=${month}-*/type=load"
//    val sc = sparkSession.sparkContext
//    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
//    sc.hadoopConfiguration.set(Parameters.SPARK_READ_DIR_RECURSIVE, "true")
    
    
    //'Name','Attend','Session_Count','ssOnline_Min', 'ssOnline_Max','ssOnline_Mean','ssOnline_Std', 'Size1Download','Diff1Download','Size1Upload'
    import sparkSession.implicits._
    countVal.join(minVal)
      .join(maxVal).map(x => x._1 -> (x._2._1._1, x._2._1._2, x._2._2))
      .join(meanVal).map(x => x._1 -> (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2))
      .join(stdVal).map(x => x._1 -> (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._2))
      .join(attendVal).map(x => (x._1, x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._2))
      .toDF("Name", "Session_Count", "ssOnline_Min", "ssOnline_Max", "ssOnline_Mean", "ssOnline_Std", "Attend")
  }
  
//  def calFor28DaysInMonth(sparkSession: SparkSession, month: String) = {
//    val path = RadiusParameters.CLASSIFY_PATH + s"/${month}-*/session"
//    val session = sparkSession.sparkContext.textFile(path, 1)
//      .map(x => new Session(x))
//      .filter(x => DateTimeUtil.create(x.day, DateTimeUtil.YMD).getDayOfMonth() <= 28)
//      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//
//    val countVal = session.map(x => x.name -> x.id).distinct().map(x => x._1 -> 1).reduceByKey(_+_)
//    val minVal = session.map(x   => x.name -> x.time).reduceByKey(Math.min)
//    val maxVal = session.map(x   => x.name -> x.time).reduceByKey(Math.max)
//    val meanVal = session.map(x  => x.name -> x.time.toDouble).groupByKey().map(x => x._1 -> mean(x._2))
//    val stdVal = session.map(x   => x.name -> x.time.toDouble).groupByKey().map(x => x._1 -> std(x._2))
//    
////    val pathLoad = s"/data/radius/classify/day=${month}-*/type=load"
////    val sc = sparkSession.sparkContext
////    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
////    sc.hadoopConfiguration.set(Parameters.SPARK_READ_DIR_RECURSIVE, "true")
//    
//    val attendVal = session.map(x => x.name -> x.day)
//      .distinct()
//      .map(x => x._1 -> 1)
//      .reduceByKey(_+_)
//
//    import sparkSession.implicits._
//    countVal.join(minVal)
//      .join(maxVal).map(x => x._1 -> (x._2._1._1, x._2._1._2, x._2._2))
//      .join(meanVal).map(x => x._1 -> (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2))
//      .join(stdVal).map(x => x._1 -> (x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._2))
//      .join(attendVal).map(x => (x._1, x._2._1._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._2))
//      .toDF("name", "Session_COUNT", "Sesion_MIN", "Sesion_MAX", "ssOnline_Mean", "ssOnline_Std", "Attend")
//  }
  
  /**
   * Calculate variance for Array
   */
  def variance(xs: Iterable[Double]): Double = {
    if (xs.size > 1) {
      val m = mean(xs)
      mean(xs.map(x => Math.pow(x - m, 2))) // <==> val sum = xs.map(x => Math.pow(x-m, 2)).sum; return (sum / xs.size)
    } else 0
  }

  def std(xs: Iterable[Double]): Double = {
     Math.sqrt(variance(xs))
  }
  
  /**
   * Calculate average for Array
   */
  def mean(xs: Iterable[Double]): Double = {
    xs.sum / xs.size
  }
}