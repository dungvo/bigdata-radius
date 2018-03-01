package com.ftel.bigdata.conf

import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.ftel.bigdata.utils.Parameters.SPARK_READ_DIR_RECURSIVE
import com.ftel.bigdata.spark.es.EsConnection
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkFiles

object Configure {
  
  val ES_HOST = "172.27.11.156"
  val ES_PORT = 9200
  
  private val CONFIG_FILE_PATH = "conf/application.conf"
  private val CONFIG_FILE_LOCAL_PATH = "conf/applicationLocal.conf"
  val DNS_CONFIG_KEY = "postgres_dns"
  //val CONFIG = ConfigFactory.parseFile(new File(CONFIG_FILE_PATH))
  val CONFIG = ConfigFactory.defaultApplication()
  //ConfigFactory.defaultApplication()

  def printAllKey() {
    CONFIG.entrySet().toArray().foreach(println)
  }
  
  def getSparkContextLocal(): SparkContext = {
    getSparkContextLocal(null)
  }

  def getSparkContextLocal(es: EsConnection): SparkContext = {
    val sparkConf = new SparkConf().setAppName("Local").setMaster(s"local[8]")
    if (es != null) es.configure(sparkConf)
    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    sc.hadoopConfiguration.set(SPARK_READ_DIR_RECURSIVE, "true")
    sc
  }
  
  def getSparkContext(): SparkContext = {
    getSparkContext(null, null)
  }
  
  def getSparkContextWithSchedulerFair(): SparkContext = {
    getSparkContext(null, "FAIR")
  }

  def getSparkContext(es: EsConnection): SparkContext = {
    getSparkContext(es, null)
  }

  private def getSparkContext(es: EsConnection, scheduler: String): SparkContext = {
    val sparkConf = new SparkConf()
    if (es != null) es.configure(sparkConf)
    if (scheduler != null) sparkConf.set("spark.scheduler.mode", scheduler)
    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set(SPARK_READ_DIR_RECURSIVE, "true")
    sc
  }
  
  def getBroadcast(sc: SparkContext, path: String): Broadcast[Array[String]] = {
    sc.broadcast(sc.textFile(path, 1).collect())
  }
  
//  def getRedisConfigForContract(): RedisConfig = {
//    
//    CONFIG.entrySet().toArray().foreach(println)
//    val conf = CONFIG.getConfig("redis.contract")
//    RedisConfig(conf.getString("host"), conf.getInt("port"), conf.getInt("db"))
//    //null
//  }
//  
////  def getElasticSearchConfig(): RedisConfig = {
////    val conf = CONFIG.getConfig("elasticsearch")
////    RedisConfig(conf.getString("host"), conf.getInt("port"), conf.getInt("db"))
////  }
//  
//  def main(args: Array[String]) {
//    val rediscontract = getRedisConfigForContract()
//    
//    println(rediscontract.host)
//    println(rediscontract.port)
//    println(rediscontract.db)
//  }
}