package batch_jobs.radius

import batch_jobs.anomaly_detection.ThresholdCalculator.getClass
import core.streaming.ParserBoacast
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import parser.{ConnLogParser, LoadLogParser}


/**
  * Created by hungdv on 24/08/2017.
  */

object ParseConnLogRadiusRaw {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val logger = Logger.getLogger(getClass)

    val sparkConf = new SparkConf()
      //.set("spark.cassandra.connection.host","172.27.11.156")

    val sparkSession = SparkSession.builder()
      .appName("batch_jobs.ParserRadiusRawLog")
      .master("yarn")
      .getOrCreate()
    val sparkContext = sparkSession.sparkContext
    val parser = new ConnLogParser()
    val bParser: Broadcast[ConnLogParser] = ParserBoacast.getInstance(sparkContext,parser)

/*    val fs = FileSystem.get(new Configuration())
    val status: Array[FileStatus] = fs.listStatus(new Path("/user/hungvd8/radius-raw/radius_rawlog/"))
    val files = status.map(x => x.getPath)*/

/*    val r: Seq[Int] = 1 to 31

    r.foreach{number =>

      val fileName = s"hdfs://ha-cluster/user/hungvd8/radius-raw/radius_1710/isc-radius-2017-10-" + f"${number}%02d"
      println(fileName)
      val date  = "2017-10-" + f"${number}%02d"
      //val date  = fileName.substring(fileName.length - 10,fileName.length)
      val rawLogs = sparkSession.sparkContext.textFile(fileName)
      val pasedLog = rawLogs.map{
        line =>
          bParser.value.simply_parse_conn(date,line)
      }.filter(x => x != null)

      pasedLog.saveAsTextFile("hdfs://ha-cluster/user/hungvd8/radius-raw/connection-logs-parsed/"+date)
      println("Done " + fileName)
    }*/

    parseMonthly(sparkSession,bParser)

    //parse(sparkSession,31,"hdfs://ha-cluster/user/hungvd8/radius-raw/radius_1705/isc-radius-2017-05-","2017-05-",bParser)

    //parse(sparkSession,30,"hdfs://ha-cluster/user/hungvd8/radius-raw/radius_rawlog/isc-radius-2017-04-","2017-04-",bParser)

  }

  def parseMonthly(sparkSession: SparkSession,bParser: Broadcast[ConnLogParser]) = {
    val days = getFirstAndLastDayOfMonth()
    val surffix = dateToSurffix(days._3)
    val month = days._3
    val path = s"hdfs://ha-cluster/user/hungvd8/radius-raw/radius_${surffix}/isc-radius-${month}-"
    parse(sparkSession ,days._1,days._2,path,days._3,bParser)
  }
  /**
    *
  Just get something like this 1711, 1712, 1801, 1802,1803 .... yymm
    */
  def dateToSurffix(date: String) = {
    val year = date.substring(2,4)
    val month = date.substring(5,7)
    val surfix = year+month
    surfix
  }

  /**
    * Get last day, first day, month format of running time, (previous month)
    * @return
    */
  private def getFirstAndLastDayOfMonth() ={
    val previousMonth = DateTime.now().minusMonths(1)
    val yearMonth = previousMonth.toString("yyyy-MM")
    val firstDay = new DateTime(previousMonth).dayOfMonth().withMinimumValue().getDayOfMonth
    val lastDay = new DateTime(previousMonth).dayOfMonth().withMaximumValue().getDayOfMonth
    (firstDay,lastDay,yearMonth)
  }



  // Default from day 1 to end of the month
  def parse(sparkSession: SparkSession,numberDayOfMonth: Int, fileNamePrefix: String,datePrefix: String,bParser: Broadcast[ConnLogParser]) : Unit = {
    val r: Seq[Int] = 1 to numberDayOfMonth

    r.foreach{number =>

      val fileName =  fileNamePrefix + f"${number}%02d"
      println(fileName)
      val date  = datePrefix + f"${number}%02d"
      //val date  = fileName.substring(fileName.length - 10,fileName.length)
      val rawLogs = sparkSession.sparkContext.textFile(fileName)
      val pasedLog = rawLogs.map{
        line =>
          bParser.value.simply_parse_conn(date,line)
      }.filter(x => x != null)

      pasedLog.saveAsTextFile("hdfs://ha-cluster/user/hungvd8/radius-raw/connection-logs-parsed/"+date)
      println("Done " + fileName)
    }
  }
  // Parse from day1 to day2

  def parse(sparkSession: SparkSession,startDate: Int,endDate: Int, fileNamePrefix: String,datePrefix: String,bParser: Broadcast[ConnLogParser]) : Unit = {
    val r: Seq[Int] = startDate to endDate

    r.foreach{number =>

      val fileName =  fileNamePrefix + f"${number}%02d"
      println(fileName)
      val date  = datePrefix + f"${number}%02d"
      //val date  = fileName.substring(fileName.length - 10,fileName.length)
      val rawLogs = sparkSession.sparkContext.textFile(fileName)
      val pasedLog = rawLogs.map{
        line =>
          bParser.value.simply_parse_conn(date,line)
      }.filter(x => x != null)

      pasedLog.saveAsTextFile("hdfs://ha-cluster/user/hungvd8/radius-raw/connection-logs-parsed/"+date)
      println("Done " + fileName)
    }
  }

}


