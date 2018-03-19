package com.ftel.bigdata.radius.batch

import org.apache.spark.sql.SparkSession
import com.ftel.bigdata.utils.Parameters
import org.apache.spark.SparkContext
import com.ftel.bigdata.radius.classify.LoadLog
import com.ftel.bigdata.radius.classify.ConLog
import com.ftel.bigdata.radius.classify.ErrLog
import org.apache.spark.rdd.RDD
import com.ftel.bigdata.radius.classify.AbstractLog
import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.utils.StringUtil
import com.ftel.bigdata.radius.classify.RawLog
import org.apache.spark.storage.StorageLevel
import com.ftel.bigdata.utils.ESUtil
import com.ftel.bigdata.radius.classify.Parser

object Driver {
  def main(args: Array[String]) {
    
//    val i = 91
//    
//    println(f"$i%02d")
//    for (i <- 0 until 24) {
//            val hourI = f"$i%02d"
//            //partitionWithRaw(sc, x, day, hour, day, hour)
//            println(hourI)
//          }
    
    val sparkSession = SparkSession.builder().getOrCreate()
    val sc = sparkSession.sparkContext
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    sc.hadoopConfiguration.set(Parameters.SPARK_READ_DIR_RECURSIVE, "true")

    val flag = args(0)
    val day = args(1)
    val hour = args(2)
    val types = args(3).split(",")
    //types.map(x => check(sc, x, day, hour, day, hour))
    //types.map(x => partition(sc, x, day, hour))
    flag match {
      case "raw" => types.map(x => {
          partitionWithRaw(sc, x, day, hour, day, hour)
        })
      case "raw-all" => types.map(x => {
          for (i <- 0 until 24) {
            val hourI = f"$i%02d"
            partitionWithRaw(sc, x, day, hour, day, hourI)
          }
        })
      case "type" => types.map(x => partition(sc, x, day, hour))
    }
    
    //partition(sc, "load", day, hour)
    //partition(sc, "con", day, hour)
    //partition(sc, "err", day, hour)
    //sc.textFile(path, 1)
    //println(Parameters.SPARK_READ_DIR_RECURSIVE)
    //DateTimeUtil.test()
  }
  
//  private def partition(sc: SparkContext, logType: String, day: String, hour: String, dayFilter: String, hourFilter: String) {
//    val previous = DateTimeUtil.create(s"${day}-${hour}", "yyyy-MM-dd-HH").minusHours(1)
//    
//    val prevDay = previous.toString("yyyy-MM-dd")
//    val prevHour = previous.toString("HH")
//    
//    val path = s"/data/radius/streaming/${logType}/${day}/${hour},/data/radius/streaming/${logType}/${prevDay}/${prevHour}"
//    val out = s"/data/radius/partition/${logType}/${dayFilter}/${hourFilter}"
//    
//    val lines: RDD[AbstractLog] = logType match {
//      case "load" => sc.textFile(path, 1).map(x => LoadLog(x))
//      case "con" => sc.textFile(path, 1).map(x => ConLog(x))
//      case "err" => sc.textFile(path, 1).map(x => ErrLog(x))
//    }
//  }
  
//  private def partition(sc: SparkContext, logType: String, day: String, hour: String) {
//    val previous = DateTimeUtil.create(s"${day}-${hour}", "yyyy-MM-dd-HH").minusHours(1)
//    
//    val prevDay = previous.toString("yyyy-MM-dd")
//    val prevHour = previous.toString("HH")
//    
//    val path = s"/data/radius/streaming/${logType}/${day}/${hour},/data/radius/streaming/${logType}/${prevDay}/${prevHour}"
//    val out = s"/data/radius/partition/${logType}/${day}/${hour}"
//    
//    val filter = (x: AbstractLog) => {
//      val date = DateTimeUtil.create(x.getTimestamp() / 1000)
//      (date.toString("yyyy-MM-dd") == day && date.toString("HH") == hour)
//    }
//
//    partition(sc, logType, path, out, filter)
//  }
  
  
  
  private def partition(sc: SparkContext, logType: String, day: String, hour: String) {
//    val previous = DateTimeUtil.create(s"${day}-${hour}", "yyyy-MM-dd-HH").minusHours(1)
//    
//    val prevDay = previous.toString("yyyy-MM-dd")
//    val prevHour = previous.toString("HH")
//    
//    val path = s"/data/radius/streaming/${logType}/${day}/${hour},/data/radius/streaming/${logType}/${prevDay}/${prevHour}"
    
    partition(sc, logType, day, hour, day, hour)
  }

//  private def partition(sc: SparkContext, logType: String, day: String, hour: String, dayFilter: String, hourFilter: String) {
//    val previous = DateTimeUtil.create(s"${day}-${hour}", "yyyy-MM-dd-HH").minusHours(1)
//    
//    val prevDay = previous.toString("yyyy-MM-dd")
//    val prevHour = previous.toString("HH")
//    
//    val path = s"/data/radius/streaming/${logType}/${day}/${hour},/data/radius/streaming/${logType}/${prevDay}/${prevHour}"
//    
//    partition(sc, logType, path, dayFilter, hourFilter)
//  }
  
  private def partitionWithRaw(sc: SparkContext, logType: String, day: String, hour: String, dayFilter: String, hourFilter: String) {
    val previous = DateTimeUtil.create(s"${day}-${hour}", "yyyy-MM-dd-HH").minusHours(1)
    
    val prevDay = previous.toString("yyyy-MM-dd")
    val prevHour = previous.toString("HH")
    
    val path = Array(
        s"/data/radius/streaming/raw/${day}/${hour}",
        s"/data/radius/streaming/raw/${prevDay}/${prevHour}"
        //s"/data/radius/streaming/${logType}/${prevDay}/${prevHour}/5*" // Lấy những phút cuối của giờ trước vì có 1 số message con nằm trong giờ này
        ).mkString(",")
    println("Input Path: " + path)
    val output = s"/data/radius/partition/${logType}/${dayFilter}/${hourFilter}"
    val filter = (x: AbstractLog) => {
      val date = DateTimeUtil.create(x.getTimestamp() / 1000)
      (date.toString("yyyy-MM-dd") == dayFilter && date.toString("HH") == hourFilter)
    }
    val lines = sc.textFile(path, 1).filter(x => StringUtil.isNotNullAndEmpty(x)).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    println("COUNT: " + lines.count)
    val rdd: RDD[AbstractLog] = logType match {
      case "load" => lines.map(x => LoadLog(x))
      case "con"  => lines.map(x => RawLog(x)).map(x => Parser.parse(x.text, x.timestamp)).filter(x => x.isInstanceOf[ConLog]).map(x => x.asInstanceOf[ConLog])
      case "err"  => lines.map(x => ErrLog(x))
      case "raw"  => lines.map(x => RawLog(x))
    }
    
    val rddCache = rdd.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    
    
    
    rddCache.filter(x => x!= null)
      .filter(filter)
      .coalesce(32, false, None).saveAsTextFile(output)
      
    // Write data to Elasticsearch for monitor
    
    //val client = ESUtil.getClient("172.27.11.156", 9200)
    //ESUtil.upset(client, "radius-tracking", "docs", Map("line" -> countLine, "parse" -> countParse), day)
    //println(s"[${day}] Line: ${countLine}, Parse: ${countParse}")
  }
  
  
  
  private def partition(sc: SparkContext, logType: String, day: String, hour: String, dayFilter: String, hourFilter: String) {
    val previous = DateTimeUtil.create(s"${day}-${hour}", "yyyy-MM-dd-HH").minusHours(1)
    
    val prevDay = previous.toString("yyyy-MM-dd")
    val prevHour = previous.toString("HH")
    
    val path = Array(
        s"/data/radius/streaming/${logType}/${day}/${hour}",
        s"/data/radius/streaming/${logType}/${prevDay}/${prevHour}"
        //s"/data/radius/streaming/${logType}/${prevDay}/${prevHour}/5*" // Lấy những phút cuối của giờ trước vì có 1 số message con nằm trong giờ này
        ).mkString(",")
    println("Input Path: " + path)
    val output = s"/data/radius/partition/${logType}/${dayFilter}/${hourFilter}"
    val filter = (x: AbstractLog) => {
      val date = DateTimeUtil.create(x.getTimestamp() / 1000)
      (date.toString("yyyy-MM-dd") == dayFilter && date.toString("HH") == hourFilter)
    }
    val lines = sc.textFile(path, 1).filter(x => StringUtil.isNotNullAndEmpty(x)).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    println("COUNT: " + lines.count)
    val rdd: RDD[AbstractLog] = logType match {
      case "load" => lines.map(x => LoadLog(x))
      case "con"  => lines.map(x => ConLog(x))
      case "err"  => lines.map(x => ErrLog(x))
      case "raw"  => lines.map(x => RawLog(x))
    }
    
    val rddCache = rdd.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    
    
    
    rddCache.filter(x => x!= null)
      .filter(filter)
      .coalesce(32, false, None).saveAsTextFile(output)
      
    // Write data to Elasticsearch for monitor
    
    //val client = ESUtil.getClient("172.27.11.156", 9200)
    //ESUtil.upset(client, "radius-tracking", "docs", Map("line" -> countLine, "parse" -> countParse), day)
    //println(s"[${day}] Line: ${countLine}, Parse: ${countParse}")
  }
  
  
  private def check(sc: SparkContext, logType: String, day: String, hour: String, dayFilter: String, hourFilter: String) {
    val previous = DateTimeUtil.create(s"${day}-${hour}", "yyyy-MM-dd-HH").minusHours(1)
    
    val prevDay = previous.toString("yyyy-MM-dd")
    val prevHour = previous.toString("HH")
    
    val path = Array(
        s"/data/radius/streaming/${logType}/${day}/${hour}",
        s"/data/radius/streaming/${logType}/${prevDay}/${prevHour}/5*" // Lấy những phút cuối của giờ trước vì có 1 số message con nằm trong giờ này
        ).mkString(",")
    
    val output = s"/data/radius/partition/${logType}/${dayFilter}/${hourFilter}"
    val filter = (x: AbstractLog) => {
      val date = DateTimeUtil.create(x.getTimestamp() / 1000)
      (date.toString("yyyy-MM-dd") == dayFilter && date.toString("HH") == hourFilter)
    }
    val lines = sc.textFile(path, 1).filter(x => StringUtil.isNotNullAndEmpty(x))

    val rdd: RDD[AbstractLog] = logType match {
      case "load" => lines.map(x => LoadLog(x))
      case "con"  => lines.map(x => ConLog(x))
      case "err"  => lines.map(x => ErrLog(x))
      case "raw"  => lines.map(x => RawLog(x))
    }

    val count = lines.count()
    val countRDD = rdd.count
    val countNotNull = rdd.filter(x => x!= null).count
    val countFilter = rdd.filter(x => x!= null).filter(filter).count
    val collect = rdd.filter(x => x!= null).map(x => DateTimeUtil.create(x.getTimestamp()/1000).toString("yyyy-MM-dd-HH") -> 1).reduceByKey(_+_).collect()
    //.filter(filter)
    println(s"=============${logType}================")
    collect.foreach(println)
    println(s"count: ${count}")
    println(s"countRDD: ${countRDD}")
    println(s"countNotNull: ${countNotNull}")
    println(s"countFilter: ${countFilter}")
    
  }
  
//  private def partition(sc: SparkContext, logType: String, path: String, output: String, filter: AbstractLog => Boolean) {
//    val lines: RDD[AbstractLog] = logType match {
//      case "load" => sc.textFile(path, 1).map(x => LoadLog(x))
//      case "con" => sc.textFile(path, 1).map(x => ConLog(x))
//      case "err" => sc.textFile(path, 1).map(x => ErrLog(x))
//    }
//    lines.filter(filter).saveAsTextFile(output)
//  }
}