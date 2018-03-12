package com.ftel.bigdata.radius.es

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import com.ftel.bigdata.radius.RadiusParameters
import com.ftel.bigdata.radius.stats.LoadStats
import com.ftel.bigdata.spark.es.EsConnection
import com.ftel.bigdata.utils.Parameters
import org.apache.spark.SparkConf
import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.radius.classify.LoadLog
import com.ftel.bigdata.radius.classify.ConLog
import com.redis.RedisClient

object Driver {

  val es = new EsConnection("172.27.11.156", 9200, "radius-index", "docs")

  def main(args: Array[String]) {
    val sparkConf = new SparkConf
    es.configure(sparkConf)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val flag = args(0)
    val day = args(1)
    flag match {
      case "day" => {
        run(sparkSession, day)
      }
      
      case "month" => {
        val dateTime = DateTimeUtil.create(day, DateTimeUtil.YMD).dayOfMonth().withMinimumValue()
        val number = dateTime.dayOfMonth().getMaximumValue()
        (0 until number).map(x => {
          runESForLoad(sparkSession, dateTime.plusDays(x).toString(DateTimeUtil.YMD))
        })
      }
      case "con-day" => {
        runESForCon(sparkSession, day)
      }
      case "stats-day" => {
        runLoadStats(sparkSession, day)
      }
      
//      case "month" => {
//        val dateTime = DateTimeUtil.create(day, DateTimeUtil.YMD).dayOfMonth().withMinimumValue()
//        val number = dateTime.dayOfMonth().getMaximumValue()
//        (0 until number).map(x => {
//          runESForLoad(sparkSession, dateTime.plusDays(x).toString(DateTimeUtil.YMD))
//        })
//      }
    }
  }

  private def run(sparkSession: SparkSession, day: String) {
    val sc = sparkSession.sparkContext
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    sc.hadoopConfiguration.set(Parameters.SPARK_READ_DIR_RECURSIVE, "true")
    val fs = FileSystem.get(sc.hadoopConfiguration)

    def getLoc(visitor: String, redis: RedisClient): (String, String) = {
      redis.select(15)
      val map = redis.hgetall1(visitor.toLowerCase()).getOrElse(Map())
      map.getOrElse("province", "??") -> map.getOrElse("region", "??")
    }
    
    val load = sc.textFile(RadiusParameters.CLASSIFY_PATH + s"/day=${day}/type=load", 1).map(x => LoadLog(x))
      .mapPartitions(p => {
        val redis = new RedisClient("172.27.11.141", 6379)
        redis.select(15)
        val res = p.map(x => {
          val map = redis.hgetall1(x.name.toLowerCase()).getOrElse(Map())
          x.toES() + 
            ("province" -> map.getOrElse("province", "??")) + 
            ("region" -> map.getOrElse("region", "??")) + 
            ("contract" -> map.getOrElse("contract", "??").toLowerCase())
        })
        
        res
      }, false)
    //val con = sc.textFile(RadiusParameters.CLASSIFY_PATH + s"/day=${day}/type=con", 1).map(x => ConLog(x))
    es.save(load, s"radius-rawlog-${day}", "load")
    //es.save(con.map(x => x.toES()), s"radius-rawlog-${day}", "con")
  }
  
  private def runLoadStats(sparkSession: SparkSession, day: String) {
    val sc = sparkSession.sparkContext
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    sc.hadoopConfiguration.set(Parameters.SPARK_READ_DIR_RECURSIVE, "true")
    val fs = FileSystem.get(sc.hadoopConfiguration)

    def getLoc(visitor: String, redis: RedisClient): (String, String) = {
      redis.select(15)
      val map = redis.hgetall1(visitor.toLowerCase()).getOrElse(Map())
      map.getOrElse("province", "??") -> map.getOrElse("region", "??")
    }
    
    val load = sc.textFile(RadiusParameters.STATS_PATH + s"/${day}/load-usage", 1).map(x => new LoadStats(x))
      .mapPartitions(p => {
        val redis = new RedisClient("172.27.11.141", 6379)
        redis.select(15)
        val res = p.map(x => {
          val map = redis.hgetall1(x.name.toLowerCase()).getOrElse(Map())
          x.toES() + 
            ("province" -> map.getOrElse("province", "??")) + 
            ("region" -> map.getOrElse("region", "??")) + 
            ("contract" -> map.getOrElse("contract", "??").toLowerCase())
        })
        
        res
      }, false)
    //val con = sc.textFile(RadiusParameters.CLASSIFY_PATH + s"/day=${day}/type=con", 1).map(x => ConLog(x))
    es.save(load, s"radius-load-${day}", "docs")
    //es.save(con.map(x => x.toES()), s"radius-rawlog-${day}", "con")
  }
  
  private def runESForLoad(sparkSession: SparkSession, day: String) {
    val sc = sparkSession.sparkContext
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    sc.hadoopConfiguration.set(Parameters.SPARK_READ_DIR_RECURSIVE, "true")
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val path = RadiusParameters.CLASSIFY_PATH + s"/day=${day}/type=load"
    //val output = RadiusParameters.STATS_PATH + s"/${day}"

    val loadLogs = sc.textFile(path, 1).map(x => LoadLog(x))

    es.save(loadLogs.map(x => x.toES()), s"radius-rawlog-${day}", "load")

  }
  
  private def runESForCon(sparkSession: SparkSession, day: String) {
    val sc = sparkSession.sparkContext
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    sc.hadoopConfiguration.set(Parameters.SPARK_READ_DIR_RECURSIVE, "true")
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val path = RadiusParameters.CLASSIFY_PATH + s"/day=${day}/type=con"
    //val output = RadiusParameters.STATS_PATH + s"/${day}"

    val loadLogs = sc.textFile(path, 1).map(x => ConLog(x))

    es.save(loadLogs.map(x => x.toES()), s"radius-rawlog-${day}", "con")

  }
}