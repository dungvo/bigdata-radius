package com.ftel.bigdata.radius.stats

import com.ftel.bigdata.radius.classify.LoadLog
import org.joda.time.DateTime
import com.ftel.bigdata.conf.Configure
import com.ftel.bigdata.radius.classify.Parser
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.utils.Parameters
import org.apache.spark.storage.StorageLevel

object Driver {

  def main(args: Array[String]) {
    
    //runLocal()
    run(args)
//    val line = "(dnfdl-161226-794,(100191406247,2125795314))"
//    val tuple = Functions.parseTuple(line)
//    println(tuple._1)
//    println(tuple._2._1)
//    println(tuple._2._2)
//    
//    val dateTime = DateTimeUtil.create("2018-02-11", DateTimeUtil.YMD).dayOfMonth().withMinimumValue()
//    val number = dateTime.dayOfMonth().getMaximumValue()
//    println(number)
//    println(dateTime.toString(DateTimeUtil.YMD))
  }

  private def run(args: Array[String]) {
    val flag = args(0)
    val day = args(1)
    val sparkSession = SparkSession.builder().getOrCreate()

    flag match {
      case "month" => {
        val dateTime = DateTimeUtil.create(day, DateTimeUtil.YMD).dayOfMonth().withMinimumValue()
        val number = dateTime.dayOfMonth().getMaximumValue()
        (0 until number).map(x => {
          run(sparkSession, dateTime.plusDays(x).toString(DateTimeUtil.YMD))
        })
      }
      case "day" => {
        run(sparkSession, day)
      }
//      case "month" => {
//        val dateTime = DateTimeUtil.create(day, DateTimeUtil.YMD).dayOfMonth().withMinimumValue()
//        val number = dateTime.dayOfMonth().getMaximumValue()
//        (0 until number).map(x => {
//          run(sparkSession, dateTime.plusDays(x).toString(DateTimeUtil.YMD))
//        })
//      }
      
      case "diff" => {
        Difference.save(sparkSession, day)
      }
      case "feature" => {
        Feature.save(sparkSession, args(1))
      }
      case "feature-merge" => {
        Feature.mergeFeatureFiles(sparkSession, args(1))
      }
      case "session-month" => {
        val dateTime = DateTimeUtil.create(day, DateTimeUtil.YMD).dayOfMonth().withMinimumValue()
        val number = dateTime.dayOfMonth().getMaximumValue()
        (0 until number).map(x => {
          runSession(sparkSession, dateTime.plusDays(x).toString(DateTimeUtil.YMD))
        })
      }
    }
  }
  
  private def run(sparkSession: SparkSession, day: String) {
    val previousDay = DateTimeUtil.create(day, "yyyy-MM-dd").minusDays(1).toString("yyyy-MM-dd")
    
    val path = s"/data/radius/classify/day=${day}/type=load,/data/radius/classify/day=${previousDay}/type=load"
    val output = s"/data/radius/stats/${day}"
    
    val sc = sparkSession.sparkContext
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    sc.hadoopConfiguration.set(Parameters.SPARK_READ_DIR_RECURSIVE, "true")
    
    //val sc = Configure.getSparkContextLocal()
    
    val loadStats = sc.textFile(path, 1).map(x => LoadLog(x)).map(x => LoadStats(x))
    
    //loadStats.saveAsTextFile(s"/data/radius/stats/${day}")
    val rdd = Functions.calculateLoad(sparkSession, loadStats, LoadStats.MAX_INT, LoadStats.THRESHOLD)
      .filter(x => DateTimeUtil.create(x.timestamp / 1000).toString("yyyy-MM-dd") == day)
      .coalesce(32, false, None)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    rdd.saveAsTextFile(output + s"/load-usage")

    // Storage session
//    rdd.map(x => (x.name.toLowerCase(), x.sessionId.toLowerCase()) -> x.sessionTime)
//             .reduceByKey(Math.max)
//             .map(x => Array(x._1._1, x._1._2, x._2).mkString("\t"))
//             .coalesce(32, false, None)
//             .saveAsTextFile(output + s"/session")
    
    
    // download/upload by day
//    rdd.filter(x => x.download >= 0 || x.upload >= 0)
//       .map(x => (x.name.toLowerCase()) -> (x.downloadUsage, x.uploadUsage))
//       .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
//       .map(x => Array(x._1, x._2._1, x._2._2).mkString("\t"))
//       .coalesce(32, false, None)
//       .saveAsTextFile(output + s"/download-upload")
    
    Session.save(rdd, day)
    DownUp.save(rdd, day)
    Difference.save(sparkSession, day)

    rdd.filter(x => x.download < 0 || x.upload < 0)
       .coalesce(32, false, None)
       .saveAsTextFile(output + s"/invalid")
    
  }

  private def runSession(sparkSession: SparkSession, day: String) {
    val previousDay = DateTimeUtil.create(day, "yyyy-MM-dd").minusDays(1).toString("yyyy-MM-dd")

    val output = s"/data/radius/stats/${day}"
    
    val sc = sparkSession.sparkContext
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    sc.hadoopConfiguration.set(Parameters.SPARK_READ_DIR_RECURSIVE, "true")
    val loadStats = sc.textFile(output + s"/load-usage", 1).map(x => new LoadStats(x))
    Session.save(loadStats, day)
  }
  
  private def runFeature(sparkSession: SparkSession, day: String) {
    val previousDay = DateTimeUtil.create(day, "yyyy-MM-dd").minusDays(1).toString("yyyy-MM-dd")
    
    //val path = s"/data/radius/classify/day=${day}/type=load,/data/radius/classify/day=${previousDay}/type=load"
    //val output = s"/data/radius/stats/${day}"
    
    val sc = sparkSession.sparkContext
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    sc.hadoopConfiguration.set(Parameters.SPARK_READ_DIR_RECURSIVE, "true")
    
    //val sc = Configure.getSparkContextLocal()
    
//    val loadStats = sc.textFile(path, 1).map(x => LoadLog(x)).map(x => LoadStats(x))
//    
//    //loadStats.saveAsTextFile(s"/data/radius/stats/${day}")
//    val rdd = Functions.calculateLoad(sparkSession, loadStats, LoadStats.MAX_INT, LoadStats.THRESHOLD)
//      .filter(x => DateTimeUtil.create(x.timestamp / 1000).toString("yyyy-MM-dd") == day)
//      .coalesce(32, false, None)
//      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//    rdd.saveAsTextFile(output + s"/load-usage")
//
//    // Storage session
//    rdd.map(x => (x.name.toLowerCase(), x.sessionId.toLowerCase()) -> x.sessionTime)
//             .reduceByKey(Math.max)
//             .map(x => Array(x._1._1, x._1._2, x._2).mkString("\t"))
//             .coalesce(32, false, None)
//             .saveAsTextFile(output + s"/session")
//    // download/upload by day
//    rdd.filter(x => x.download >= 0 || x.upload >= 0)
//       .map(x => (x.name.toLowerCase()) -> (x.downloadUsage, x.uploadUsage))
//       .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
//       .map(x => Array(x._1, x._2._1, x._2._2).mkString("\t"))
//       .coalesce(32, false, None)
//       .saveAsTextFile(output + s"/download-upload")
//       
//    rdd.filter(x => x.download < 0 || x.upload < 0)
//       .coalesce(32, false, None)
//       .saveAsTextFile(output + s"/invalid")
  }
  
  /*
  private def runDiff(sparkSession: SparkSession, day: String) {
    val previousDay = DateTimeUtil.create(day, "yyyy-MM-dd").minusDays(1).toString("yyyy-MM-dd")

    val path = s"/data/radius/stats/${previousDay}/download-upload,/data/radius/stats/${day}/download-upload"

    val sc = sparkSession.sparkContext
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    sc.hadoopConfiguration.set(Parameters.SPARK_READ_DIR_RECURSIVE, "true")
    
    //val sc = Configure.getSparkContextLocal()
    
    val loadStats = sc.textFile(path, 1).map(x => LoadLog(x)).map(x => LoadStats(x))
    
    //loadStats.saveAsTextFile(s"/data/radius/stats/${day}")
    val rdd = Functions.calculateLoad(sparkSession, loadStats, LoadStats.MAX_INT, LoadStats.THRESHOLD).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    rdd.filter(x => DateTimeUtil.create(x.timestamp / 1000).toString("yyyy-MM-dd") == day)
       .coalesce(32, false, None)
       .saveAsTextFile(output + s"/load-usage")

    // Storage session
    rdd.map(x => (x.name.toLowerCase(), x.sessionId.toLowerCase()) -> x.sessionTime)
             .reduceByKey(Math.max)
             .coalesce(32, false, None)
             .saveAsTextFile(output + s"/session")
    // download/upload by day
    rdd.filter(x => x.download >= 0 || x.upload >= 0)
       .map(x => (x.name.toLowerCase()) -> (x.downloadUsage, x.uploadUsage))
       .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
       .coalesce(32, false, None)
       .saveAsTextFile(output + s"/download-upload")
       
    rdd.filter(x => x.download < 0 || x.upload < 0)
       .coalesce(32, false, None)
       .saveAsTextFile(output + s"/invalid")
  }
  */
  private def runLocal() {
    val MAX_INT = 1500
    val THRESHOLD = 1100
    val data = Array(
      """ "ACTALIVE","Dec 01 2017 06:59:59","LDG-MP01-2","796176075","Lddsl-161001-360","1","100","1200","0","1011598","100.91.231.187","64:d9:54:82:37:e4","","0","0","0","0","0","0" """,
      """ "ACTALIVE","Dec 01 2017 08:59:59","HCM-MP01-1","1670917756","sgfdl-151006-056","1","90","900","0","1","118.69.111.14","00:0d:48:0e:33:02","","0","0","0","0","0","0" """,
      """ "ACTALIVE","Dec 01 2017 09:59:59","LDG-MP01-2","-1655374348","Lddsl-161001-360","1","50","500","0","445786","100.91.228.125","64:d9:54:bf:b8:28","","0","0","0","0","0","0" """,
      """ "ACTALIVE","Dec 01 2017 10:59:59","LDG-MP01-2","1309903068","sgfdl-151006-056","1","60","1200","0","445789","100.91.228.118","a0:f3:c1:42:41:f7","","0","0","0","0","0","0" """,
      """ "ACTALIVE","Dec 01 2017 11:59:59","HCM-MP06-1","192765439","sgfdl-151006-056","1","30","120","0","323988","183.80.167.92","4c:f2:bf:69:ea:72","2405:4800:5a84:df95:0000:0000:0000:0000/64","0","0","1246050250","0","3191306576","1" """,
      """ "ACTLOGOF","Dec 01 2017 12:59:59","DNG-MP01-1","-247656230","Lddsl-161001-360","1","0","120","10","48293","100.99.86.7","70:d9:31:c4:05:de","2405:4800:309f:1db5:0000:0000:0000:0000/64","0","0","119470252","0","2195580203","0" """,
      """ "ACTALIVE","Dec 01 2017 13:59:59","HCM-MP06-1","-930271376","Lddsl-161001-360","2","10","100","0","323994","1.54.136.213","4c:f2:bf:43:2b:0a","2405:4800:5a84:df92:0000:0000:0000:0000/64","0","0","1714387855","0","1813300587","5" """,
      """ "ACTALIVE","Dec 01 2017 14:59:59","HCM-MP06-1","732492071","sgfdl-151006-056","2","80","1100","0","539973","100.106.36.93","70:d9:31:6e:d1:de","2405:4800:5a97:0925:0000:0000:0000:0000/64","0","0","411622520","0","3380674348","1" """,
      """ "ACTALIVE","Dec 01 2017 15:59:59","HCM-MP06-1","1846345404","Lddsl-161001-360","2","70","1000","0","712771","42.116.210.125","4c:f2:bf:77:c8:b6","2405:4800:5a84:c533:0000:0000:0000:0000/64","0","0","191347017","1","4051559148","31" """)
    
    val day = "2017-12-01"
    val path = "radius-log-sample.csv"
    val sparkSession = SparkSession.builder().appName("local").master("local[4]").getOrCreate()
    val sc = sparkSession.sparkContext

    val lines = sc.parallelize(data, 1) //sc.textFile(path, 1)
    val logs = lines.map(x => Parser.parse(x, day)).filter(x => x.isInstanceOf[LoadLog])

    val loadStats = logs.map(x => LoadStats(x.asInstanceOf[LoadLog]))
    
    val rdd = Functions.calculateLoad(sparkSession, loadStats, MAX_INT, THRESHOLD)
    //val encoder = Encoders.bean(classOf[LoadStats])

    //val rdd = df.as[LoadStats](encoder).rdd

    rdd.foreach(println)//.asInstanceOf[Dataset[LoadStats]]
  }
  
  
}