package streaming_jobs.dns

import batch_jobs.radius.ParsedLoadLogRadiusRaw.getClass
import core.streaming.RedisClusterClientFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Duration}
import redis.clients.jedis.{Jedis, JedisCluster}
import streaming_jobs.dns.DNS_Mapping_Batch_Fix_Missing.getClass
import util.PathController

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by hungdv on 03/11/2017.
  */
class DNS_Mapping_Batch extends Serializable{
  private var rootInputFolder = ""
  private var rootOutputFolder = ""
  private val DNS_PREFIX = "*"
  private val DNS_SURFIX = "*"
  private val DNS_OUTPUT_SURFIX = "out"
  private var dNSConfig = DNSConfig()
  private val bNDSParser = new DNSParser()
  private var sparkSession: SparkSession = _
  private var dateTime: DateTime = _
  private val INCORRECT_FORMAT = "-2"

  def this(sparkSession: SparkSession,inputPath: String, outputPath: String,dateTime: DateTime) ={
    this()
    this.sparkSession = sparkSession
    this.rootInputFolder = inputPath
    this.rootOutputFolder = outputPath
    this.dateTime = dateTime
  }


  def parseAndMap() = {

    val raw = loadDNSRawRDD()
    val result = raw.mapPartitions{part =>
    //val result = raw.map{line =>
      val redisClient: JedisCluster = RedisClusterClientFactory.getOrCreateClient(dNSConfig.redisCluster)
      part.map{line =>
        val parsedObject = bNDSParser.parseRsync(line)
        if(parsedObject._1 != -1){
          val candidates = redisClient.lrange(parsedObject._2,0,-1).toList
          val name = RedisHelper.mapping(candidates,parsedObject._1)
          line + "\t" + name
        }
        else{
          line + INCORRECT_FORMAT
        }
      }
    }
    result
  }
  def saveToHDFS(df :DataFrame) = {

  }
  def saveTOHDFS(rdd: RDD[String]) = {
    rdd.saveAsTextFile(PathController.convertDateTimeToFilePath(this.rootOutputFolder,date = dateTime,this.DNS_PREFIX,
      this.DNS_OUTPUT_SURFIX))
  }
  def saveTOHDFS_DF(rdd: RDD[String],sparkSession: SparkSession) = {
    import sparkSession.implicits._
    rdd.toDF.write.mode("overwrite").save(PathController.convertDateTimeToFilePath(this.rootOutputFolder,date = dateTime,this.DNS_PREFIX,
      this.DNS_OUTPUT_SURFIX))
  }
  def loadDNSRaw(): DataFrame ={
    sparkSession.read.format("csv").option("header","true")
      .csv(PathController.convertDateTimeToFilePath(this.rootInputFolder,this.dateTime,this.DNS_PREFIX,this.DNS_SURFIX))
  }
  def loadDNSRawRDD(): RDD[String] = {
    println("LOAD DATA")
    val path = PathController.convertDateTimeToFilePath(this.rootInputFolder,this.dateTime)
    val file = PathController.convertDateTimeToFilePath(this.rootInputFolder,this.dateTime,this.DNS_PREFIX,this.DNS_SURFIX)
    println("PATH : "+path)
    println("FILE : "+file)
    import org.apache.hadoop.fs._
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    if(fs.exists(new Path(path))){
      println("PATH EXIST")
      sparkSession.sparkContext.textFile(file)
    }else{
      println("File does not exist")
      sparkSession.sparkContext.emptyRDD[String]
    }
  }


}

object DNS_Mapping_Batch {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val logger = Logger.getLogger(getClass)
    val sparkConf = new SparkConf()
    val dateTime = util.DatetimeController.getNow()
    val sparkSession = SparkSession.builder()
      .appName("batch_jobs.MappingDNS_"+dateTime)
      //.master("local[4]")
      .master("yarn")
      .getOrCreate()
    val dNSConfig = DNSConfig
    val mapper: DNS_Mapping_Batch = new DNS_Mapping_Batch(sparkSession,
      "hdfs://ha-cluster/data/dns/dns-raw-two-hours",
      //"/data/dns/dns-raw-two-hours",
      "hdfs://ha-cluster/data/dns/dns-extracted-two-hours",
      //"/data/dns/dns-extracted-two-hours",
      new DateTime(DateTime.now().minusHours(3)))

    val result = mapper.parseAndMap()
    mapper. saveTOHDFS_DF(result,sparkSession)
    println("Done ")
  }
  def getDateList(startDate: DateTime,endDate: DateTime): ArrayBuffer[DateTime]={
    var result =  new ArrayBuffer[DateTime]()
    val duration = new Duration(startDate,endDate).getStandardHours.toInt
    println(duration)
    for(i <- 0 to duration by 2){
      val currentDate = startDate.plusHours(i)
      result.append(currentDate)
    }
    result

  }
}

object DNS_Mapping_Batch_Fix_Missing {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val logger = Logger.getLogger(getClass)
    val sparkConf = new SparkConf()
    val dateTime = util.DatetimeController.getNow()
    val sparkSession = SparkSession.builder()
      .appName("batch_jobs.MappingDNS_"+dateTime)
      //.master("local[4]")
      .master("yarn")
      .getOrCreate()
    val dNSConfig = DNSConfig


    val startDate =  stringToDatime("2017-12-18 15","yyyy-MM-dd HH")
    val endDate =  stringToDatime("2017-12-18 15","yyyy-MM-dd HH")
    val dateList = getDateList(startDate,endDate)
    dateList.foreach{
      dateTime =>
        val mapper: DNS_Mapping_Batch = new DNS_Mapping_Batch(sparkSession,
          "hdfs://ha-cluster/data/dns/dns-raw-two-hours",
          //"/data/dns/dns-raw-two-hours",
          "hdfs://ha-cluster/data/dns/dns-extracted-two-hours",
          //"/data/dns/dns-extracted-two-hours",
          dateTime.minusHours(3))

        println("Process : " + dateTime.toString())
        val result = mapper.parseAndMap()
        mapper. saveTOHDFS_DF(result,sparkSession)
    }
    println("Done ")

  }
  def getDateList(startDate: DateTime,endDate: DateTime): ArrayBuffer[DateTime]={
    var result =  new ArrayBuffer[DateTime]()
    val duration = new Duration(startDate,endDate).getStandardHours.toInt
    println(duration)
    for(i <- 0 to duration by 2){
      val currentDate = startDate.plusHours(i)
      result.append(currentDate)
    }
    result

  }
  def stringToDatime(string: String,pattern: String): DateTime ={
    val date: DateTime = DateTime.parse(string,DateTimeFormat.forPattern(pattern) )
    date
  }
}

object wtf_test{
    def main(args: Array[String]): Unit = {

      val startDate =  stringToDatime("2017-11-24 13","yyyy-MM-dd HH")
      val endDate =  stringToDatime("2017-11-27 09","yyyy-MM-dd HH")
      val dateList = getDateList(startDate,endDate)
      dateList.foreach{
        dateTime =>
           println( dateTime.minusHours(3))
      }
      println("Done ")

    }
    def getDateList(startDate: DateTime,endDate: DateTime): ArrayBuffer[DateTime]={
      var result =  new ArrayBuffer[DateTime]()
      val duration = new Duration(startDate,endDate).getStandardHours.toInt
      println(duration)
      for(i <- 0 to duration by 2){
        val currentDate = startDate.plusHours(i)
        result.append(currentDate)
      }
      result

    }
    def stringToDatime(string: String,pattern: String): DateTime ={
      val date: DateTime = DateTime.parse(string,DateTimeFormat.forPattern(pattern) )
      date
    }

}
