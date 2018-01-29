package com.ftel.bigdata.radius.dns

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object Driver {
  def main(args: Array[String]): Unit = {
    
//    val logger = Logger.getLogger(getClass)
//    val sparkConf = new SparkConf()
//    val dateTime = util.DatetimeController.getNow()
//    val sparkSession = SparkSession.builder()
//      .appName("batch_jobs.MappingDNS_"+dateTime)
//      //.master("local[4]")
//      .master("yarn")
//      .getOrCreate()
//    val dNSConfig = DNSConfig
//    val mapper: DNS_Mapping_Batch = new DNS_Mapping_Batch(sparkSession,
//      "hdfs://ha-cluster/data/dns/dns-raw-two-hours",
//      //"/data/dns/dns-raw-two-hours",
//      "hdfs://ha-cluster/data/dns/dns-extracted-two-hours",
//      //"/data/dns/dns-extracted-two-hours",
//      new DateTime(DateTime.now().minusHours(3)))
//
//    val result = mapper.parseAndMap()
//    mapper. saveTOHDFS_DF(result,sparkSession)
//    println("Done ")
//  }
//  def getDateList(startDate: DateTime,endDate: DateTime): ArrayBuffer[DateTime]={
//    var result =  new ArrayBuffer[DateTime]()
//    val duration = new Duration(startDate,endDate).getStandardHours.toInt
//    println(duration)
//    for(i <- 0 to duration by 2){
//      val currentDate = startDate.plusHours(i)
//      result.append(currentDate)
//    }
//    result

  }
  
  
  private def getLines(sc: SparkContext): RDD[String] = {
    //sparkSession.sparkContext.textFile(file)
    null
  }
}