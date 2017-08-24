package batch_jobs.radius

import batch_jobs.anomaly_detection.ThresholdCalculator.getClass
import core.streaming.ParserBoacast
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import parser.ConnLogParser


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
    val bParser = ParserBoacast.getInstance(sparkContext,parser)

/*    val fs = FileSystem.get(new Configuration())
    val status: Array[FileStatus] = fs.listStatus(new Path("/user/hungvd8/radius-raw/radius_rawlog/"))
    val files = status.map(x => x.getPath)*/
    val r: Seq[Int] = 1 to 31

    r.foreach{number =>

      val fileName = "/user/hungvd8/radius-raw/radius_rawlog/isc-radius-2017-07-" + f"${number}%02d"
      println(fileName)
      val date  = "2017-07-" + f"${number}%02d"
      //val date  = fileName.substring(fileName.length - 10,fileName.length)
      val rawLogs = sparkSession.sparkContext.textFile(fileName)
      val pasedLog = rawLogs.map{
        line =>
          bParser.value.simply_parse_conn(date,line)
      }.filter(x => x != null)

      pasedLog.saveAsTextFile("/user/hungvd8/radius-raw/connection-logs-parsed/"+date)
      println("Done " + fileName)
    }


  }

}
