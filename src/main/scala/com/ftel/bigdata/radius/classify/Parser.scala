package com.ftel.bigdata.radius.classify

import com.ftel.bigdata.conf.Configure
import com.ftel.bigdata.utils.RDDMultipleTextOutputFormat.save
import org.apache.spark.SparkContext
import com.ftel.bigdata.utils.ESUtil
import com.ftel.bigdata.utils.DateTimeUtil

object Parser {
  
  val DATE_TIME_PATTERN_NOMALIZE = "yyyy-MM-dd HH:mm:ss" // "Dec 01 2017 06:59:59"
  private val PARTITION_SIZE = 256

  def run(sc: SparkContext, timestamp: Long) {
    try {
      val day = DateTimeUtil.create(timestamp / 1000).toString(DateTimeUtil.YMD)
      val month = day.substring(0, 7)
      val path = s"/data/radius/rawlog/${month}/isc-radius-${day}"
      val output = s"/data/radius/temp/classify/${day}"
      //val sc = Configure.getSparkContext()
      val lines = sc.textFile(path, 1).persist()

      val logs = lines.map(x => Parser.parse(x, timestamp)).persist()
      val errLog = logs.filter(x => x.isInstanceOf[ErrLog])
      //val loadLog = logs.filter(x => x.isInstanceOf[ErrLog])
      //val conLog = logs.filter(x => x.isInstanceOf[ErrLog])

      val nonErrLog = logs.filter(x => !x.isInstanceOf[ErrLog])

      val f = (x: AbstractLog) => x.getKey() + "-" + day
      /**
       * NOTE: If don't use PARTITION_SIZE in this method or PARTITION_SIZE so small,
       * this method can run into problem: ***Self-suppression not permitted***
       */
      //save[AbstractLog](loadLog, f, output, PARTITION_SIZE)
      //save[AbstractLog](conLog, f, output, PARTITION_SIZE)
      save[AbstractLog](nonErrLog, f, output, PARTITION_SIZE)
      errLog.saveAsTextFile(s"${output}/day=${day}/type=err")

      val countLine = lines.count()
      val countParse = sc.textFile(s"${output}", 1).count()

      val client = ESUtil.getClient("172.27.11.156", 9200)
      ESUtil.upset(client, "radius-tracking", "docs", Map("line" -> countLine, "parse" -> countParse), day)
      println(s"[${day}] Line: ${countLine}, Parse: ${countParse}")

      assert(countLine == countParse, s"[${day}] Log and Parse is not equal [Line: ${countLine}, Parse: ${countParse}]")
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  private def runTest(args: Array[String]) {
    val day = args(0)
    val path = s"/data/radius/rawlog/isc-radius-${day}"
    val path2 = s"/data/radius/classify/${day}/test"
    val sc = Configure.getSparkContext()
    val lines = sc.textFile(path, 1).map(x => x -> 1).persist()
    val lines2 = sc.textFile(path2, 1).map(x => x -> 2).persist()
    lines.join(lines2).filter(x => (x._2._1.toInt + x._2._2.toInt) == 2).saveAsTextFile("/data/radius/compare")
    //val logs = lines.map(x => Parser(x, day)).persist()//.filter(x => !x.isInstanceOf[ErrLog])
    //logs.saveAsTextFile(output + "/test")
    //val f = (x: AbstractLog) => x.getKey() + "-" + day
    /**
     * NOTE: If don't use PARTITION_SIZE in this method or PARTITION_SIZE so small, 
     * this method can run into problem: ***Self-suppression not permitted***
     */
    //save[AbstractLog](logs, f, output, PARTITION_SIZE)
    
    
//    println("LINES: " + lines.count())
//    println("LOG: " + logs.count())
//    val result = logs.map(x => x.get() -> 1).reduceByKey(_+_).collect()//.foreach(println)
////    println("SIZE: " + lines.count())
//    result.foreach(println)
    
  }

  def runLocal() {
    //val path = "/data/radius/isc-radius-2017-12-01"
    val timestamp = 1512086400L//"2017-12-01"
    val path = "radius-log-sample.csv"
    val sc = Configure.getSparkContextLocal()

    val lines = sc.textFile(path, 1)
    val logs = lines.map(x => Parser.parse(x, timestamp))
    
    println("===================")
    logs.foreach(println)
    //logs.map(x => x.get() -> 1).reduceByKey(_+_).foreach(println)
    //val f = (x: AbstractLog) => x.getKey() + "-" + day
    //save(logs, f, "./output/radius")
    //logs.foreach(println)
    /**
     * PATH: "radius-log-sample.csv"
     * (ConLog-Reject,28)
     * (LoadLog,60)
     * (ErrLog,2)
     * (ConLog-LogOff,22)
     * (ConLog-SignIn,9)
     */
    //logs.map(x => )
    //println(loadLogs.count())
    //lines.foreach(println)

    //"ACTALIVE","Dec 01 2017 06:59:59","LDG-MP01-2","796176075","Lddsl-161001-360","1905765","477268962","3712614232","0","1011598","100.91.231.187","64:d9:54:82:37:e4","","1","35","0","0","0","0"
//    println("LINES: " + lines.count())
//    println("LOG: " + logs.count())
//    val result = logs.map(x => x.get() -> 1).reduceByKey(_+_).collect()//.foreach(println)
////    println("SIZE: " + lines.count())
//    result.foreach(println)
//    
//    logs.filter(x => x.isInstanceOf[ErrLog]).foreach(println)
  }
  
  def parse(line: String, timestamp: Long): AbstractLog = {
    try {
      val loadLog = LoadLog(line, timestamp)
      if (loadLog.isInstanceOf[ErrLog]) {
        val con = ConLog(line, timestamp)
        //println(con.toString())
        con
      } else {
        loadLog
      }
    } catch {
      case e: Exception => /*println("[Parse] " + line); */{
        //println(line)
        new ErrLog(timestamp, line)
      }
    }
  }
  
  def main(args: Array[String]) {
    val path = "error.csv"
    val sc = Configure.getSparkContextLocal()
    
    def getType(x: String) = {
      val arr = x.split("\",\"")
      if (arr.size == 11) "LOAD-11"
      else if (x.split(" ").size == 6) "DISCARD"
      else "UNKNOW"
    }
    val lines = sc.textFile(path, 1).map(x => getType(x) -> 1).reduceByKey(_+_).foreach(println)
    val count = sc.textFile(path, 1).map(x => parse(x, 1512086400L)).filter(x => x.isInstanceOf[LoadLog]).count()
    println(count)
  }
}