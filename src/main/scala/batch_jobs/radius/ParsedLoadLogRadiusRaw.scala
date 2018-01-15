package batch_jobs.radius

import batch_jobs.radius.ParseConnLogRadiusRaw.getClass
import batch_jobs.radius.ParsedLoadLogRadiusRaw.getClass
import core.streaming.{LoadLogBroadcast, ParserBoacast}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructType}
import org.joda.time.DateTime
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
      //.master("local[*]")
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
    //parse(sparkSession,1,30,"hdfs://ha-cluster/user/hungvd8/radius-raw/radius_1711/isc-radius-2017-11-","2017-11",bParser)
    parseMonthly(sparkSession,bParser)
    //parseFixt5(sparkSession,31,"hdfs://ha-cluster/user/hungvd8/radius-raw/radius_1705/isc-radius-2017-05-","2017-05",bParser)

    //quickFixErrorLog(sparkSession,0,"hdfs://ha-cluster/user/hungvd8/radius-raw/radius-load-log-2017-05-131415/201705","2017-05",bParser)

  }

  def parseMonthly(sparkSession: SparkSession,bParser: Broadcast[LoadLogParser]) = {
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
    val now = DateTime.now().minusMonths(1)
    val yearMonth = now.toString("yyyy-MM")
    val firstDay = new DateTime().dayOfMonth().withMinimumValue().getDayOfMonth
    val lastDay = new DateTime().dayOfMonth().withMaximumValue().getDayOfMonth
    (firstDay,lastDay,yearMonth)
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



object testJson{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val logger = Logger.getLogger(getClass)
    val sparkConf = new SparkConf()
    val sparkSession = SparkSession.builder()
      .appName("batch_jobs.ParserRadiusRawLog_LoadLog")
      .master("local[2]")
      //.master("yarn")
      .getOrCreate()
    val sparkContext = sparkSession.sparkContext




    val netFlow = new StructType()
      .add("netflow",new StructType()
        .add("dst_as", LongType )
        .add("dst_mask", LongType  )
        .add("engine_id", LongType  )
        .add("engine_type", LongType  )
        .add("first_switched", StringType  )
        .add("flow_records", LongType   )
        .add("flow_seq_num", LongType  )
        .add("in_bytes", LongType  )
        .add("in_pkts", LongType  )
        .add("input_snmp", LongType  )
        .add("ipv4_dst_addr", StringType  )
        .add("ipv4_next_hop", StringType  )
        .add("ipv4_src_addr", StringType  )
        .add("l4_dst_port", LongType  )
        .add("l4_src_port", LongType  )
        .add("last_switched", StringType  )
        .add("output_snmp", LongType )
        .add("protocol", LongType )
        .add("sampling_algorithm", LongType )
        .add("sampling_interval", LongType )
        .add("src_as", LongType )
        .add("src_mask", LongType )
        .add("src_tos", LongType )
        .add("tcp_flags", LongType)
        .add("version", LongType ))



    val json = sparkSession.read
        //.schema(struct)
      .schema(netFlow)
      .json("/home/hungdv/Desktop/json.sample")
    val schema = json.schema
    val dateTime  =   org.joda.time.DateTime.now().minusDays(1).toString("yyyyMMdd")
    println(dateTime)
    json.repartition()
    //json.select("netflow.*").show()
  }
}