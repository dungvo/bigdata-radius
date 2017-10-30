package batch_jobs.radius

import batch_jobs.radius.ParseConnLogRadiusRaw.getClass
import core.streaming.{LoadLogBroadcast, ParserBoacast}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import parser.{ConnLogParser, LoadLogParser}

/**
  * Created by hungdv on 08/09/2017.
  */
object ParsedLoadLogRadiusRaw {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val logger = Logger.getLogger(getClass)
    val sparkConf = new SparkConf()
    val sparkSession = SparkSession.builder()
      .appName("batch_jobs.ParserRadiusRawLog_LoadLog")
      .master("yarn")
      .getOrCreate()
    val sparkContext = sparkSession.sparkContext
    val parser = new LoadLogParser
    val bParser: Broadcast[LoadLogParser] = LoadLogBroadcast.getInstance(sparkContext,parser)

    /*    val fs = FileSystem.get(new Configuration())
        val status: Array[FileStatus] = fs.listStatus(new Path("/user/hungvd8/radius-raw/radius_rawlog/"))
        val files = status.map(x => x.getPath)*/
/*
    val r: Seq[Int] = 1 to 30

    r.foreach{number =>

      val fileName = "hdfs://ha-cluster/user/hungvd8/radius-raw/radius_1706/isc-radius-2017-06-" + f"${number}%02d"
      println(fileName)
      val date  = "2017-06-" + f"${number}%02d"
      //val date  = fileName.substring(fileName.length - 10,fileName.length)
      val rawLogs = sparkSession.sparkContext.textFile(fileName)
      val pasedLog = rawLogs.map{
        line =>
          bParser.value.extractValues(line)
      }.filter(x => x != null)

      pasedLog.saveAsTextFile("hdfs://ha-cluster/user/hungvd8/radius-raw/connection-logs-parsed/"+date)
      println("Done " + fileName)

    }
*/
    //parse(sparkSession,31,"hdfs://ha-cluster/user/hungvd8/radius-raw/radius_rawlog/isc-radius-2017-07-","2017-07",bParser)
    //parse(sparkSession,30,"hdfs://ha-cluster/user/hungvd8/radius-raw/radius_1706/isc-radius-2017-06-","2017-06",bParser)
    parse(sparkSession,1,31,"hdfs://ha-cluster/user/hungvd8/radius-raw/radius_1708/isc-radius-2017-08-","2017-08",bParser)
    //parseFixt5(sparkSession,31,"hdfs://ha-cluster/user/hungvd8/radius-raw/radius_1705/isc-radius-2017-05-","2017-05",bParser)

    //quickFixErrorLog(sparkSession,0,"hdfs://ha-cluster/user/hungvd8/radius-raw/radius-load-log-2017-05-131415/201705","2017-05",bParser)

  }

  def parse(sparkSession: SparkSession,numberDayOfMonth: Int, fileNamePrefix: String,datePrefix: String,bParser: Broadcast[LoadLogParser]) : Unit = {
    val r: Seq[Int] = 1 to numberDayOfMonth

    r.foreach{number =>

      val fileName =  fileNamePrefix + f"${number}%02d"
      println(fileName)
      val date  = datePrefix +"-" +f"${number}%02d"
      //val date  = fileName.substring(fileName.length - 10,fileName.length)
      val rawLogs = sparkSession.sparkContext.textFile(fileName)
      val pasedLog = rawLogs.map{
        line =>
          bParser.value.extractValuesSimply(line)
      }.filter(x => x != null)

      pasedLog.saveAsTextFile("hdfs://ha-cluster/user/hungvd8/radius-raw/load-logs-parsed/"+date)
      //TODO chu y cai nay parse conn.
      //pasedLog.saveAsTextFile("hdfs://ha-cluster/user/hungvd8/radius-raw/load-logs-parsed/"+datePrefix+"/"+date)
      println("Done " + fileName)
    }
  }
  def parse(sparkSession: SparkSession,startDate: Int,endDate: Int, fileNamePrefix: String,datePrefix: String,bParser: Broadcast[LoadLogParser]) : Unit = {
    val r: Seq[Int] = startDate to endDate

    r.foreach{number =>

      val fileName =  fileNamePrefix + f"${number}%02d"
      println(fileName)
      val date  = datePrefix +"-" +f"${number}%02d"
      //val date  = fileName.substring(fileName.length - 10,fileName.length)
      val rawLogs = sparkSession.sparkContext.textFile(fileName)
      val pasedLog = rawLogs.map{
        line =>
          bParser.value.extractValuesSimply(line)
      }.filter(x => x != null)

      pasedLog.saveAsTextFile("hdfs://ha-cluster/user/hungvd8/radius-raw/load-logs-parsed/"+date)
      //TODO chu y cai nay parse conn.
      //pasedLog.saveAsTextFile("hdfs://ha-cluster/user/hungvd8/radius-raw/load-logs-parsed/"+datePrefix+"/"+date)
      println("Done " + fileName)
    }
  }
  def quickFixErrorLog(sparkSession: SparkSession,numberDayOfMonth: Int, fileNamePrefix: String,datePrefix: String,bParser: Broadcast[LoadLogParser]) : Unit = {
    val r: Seq[Int] = 13 to 15

    r.foreach{number =>

      val fileName =  fileNamePrefix + f"${number}%02d"+".*"
      println(fileName)
      val date  = datePrefix +"-" +f"${number}%02d"
      //val date  = fileName.substring(fileName.length - 10,fileName.length)
      val rawLogs = sparkSession.sparkContext.textFile(fileName)
      val pasedLog = rawLogs.map{
        line =>
          bParser.value.extractValuesSimply(line)
      }.filter(x => x != null)

      pasedLog.saveAsTextFile("hdfs://ha-cluster/user/hungvd8/radius-raw/load-logs-parsed/"+datePrefix+"/"+date)
      println("Done " + fileName)
    }
  }
  def parseFixt5(sparkSession: SparkSession,numberDayOfMonth: Int, fileNamePrefix: String,datePrefix: String,bParser: Broadcast[LoadLogParser]) : Unit = {
    val r: Seq[Int] = 16 to numberDayOfMonth

    r.foreach{number =>

      val fileName =  fileNamePrefix + f"${number}%02d"
      println(fileName)
      val date  = datePrefix +"-" +f"${number}%02d"
      //val date  = fileName.substring(fileName.length - 10,fileName.length)
      val rawLogs = sparkSession.sparkContext.textFile(fileName)
      val pasedLog = rawLogs.map{
        line =>
          bParser.value.extractValuesSimply(line)
      }.filter(x => x != null)

      pasedLog.saveAsTextFile("hdfs://ha-cluster/user/hungvd8/radius-raw/load-logs-parsed/"+datePrefix+"/"+date)
      println("Done " + fileName)
    }
  }

}
