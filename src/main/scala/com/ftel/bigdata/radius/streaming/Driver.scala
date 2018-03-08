package com.ftel.bigdata.radius.streaming

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.elasticsearch.spark.rdd.EsSpark

import com.ftel.bigdata.conf.Configure
import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.utils.ESUtil
import com.ftel.bigdata.utils.Parameters
import com.redis.RedisClient
import org.apache.spark.streaming.dstream.DStream
import com.ftel.bigdata.radius.classify.Parser
import com.ftel.bigdata.radius.classify.LoadLog
import com.ftel.bigdata.radius.classify.ConLog
import com.ftel.bigdata.radius.classify.ErrLog
import org.apache.spark.storage.StorageLevel
import scala.util.Try
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.ftel.bigdata.utils.RDDMultipleTextOutputFormat.save
import com.ftel.bigdata.radius.classify.AbstractLog
import core.streaming.RedisClusterClientFactory
import com.ftel.bigdata.spark.es.EsConnection
import com.ftel.bigdata.radius.classify.RawLog



//import scala.reflect.internal.Trees.ForeachPartialTreeTraverser

/**
 * {
 * "ID": "FTELHCM001",
 * "StationMAC": "74:E2:8C:D9:D8:14",
 * "FirstTimeSeen": "2017-10-12 06:18:58",
 * "LastTimeSeen": "2017-10-12 06:20:05",
 * "Power": -80,
 * "#packets": 10,
 * "BSSID": "04:18:D6:22:C6:F5",
 * "ESSID": "",
 * "ProbedESSIDs": "",
 * "#probes": 0,
 * "wlan_type": "CL",
 * "timestamp": "1507789205"
 * }
 */
case class IOT(
  id: String,
  mac: String,
  first: String,
  last: String,
  power: Long,
  packets: Long,
  bssid: String,
  essid: String,
  probedESSIDs: String,
  probes: Long,
  wlan_type: String,
  timestamp: Long)

/**
 * Entry Point
 */
object Driver {

  private val EXPIRY = 60 * 5 // 5 minutes
  private val REDIS_HOST = "172.27.11.141"
  private val REDIS_PORT = 6371

  def main(args: Array[String]) {

    //val

    //KafkaStreaming.run(brokers, topic, offsetsString, maxRate, durations, f)
    //val date = "2018-02-27T11:19:00.282Z"

    //println(DateTimeUtil.create(date, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").toString())
    //runLocal()

    run(args)
  }

  private def runLocal() {
    val brokers: String = "172.27.11.75:9092,172.27.11.80:9092,172.27.11.85:9092"
    val topic: String = "radius"
    val maxRate: Int = 1000
    val durations: Int = 60
    
    run(Array(brokers, topic, maxRate.toString, durations.toString), true)
  }
  
  private def run(args: Array[String]) {
    run(args, false)
  }
  
  private def run(args: Array[String], local: Boolean) {
    
    
    
    // Kafka Information
    val brokers: String = args(0) //"172.27.11.75:9092,172.27.11.80:9092,172.27.11.85:9092"
    val topic: String = args(1) // "radius"
    //val offsetsString = null//readCheckPoint()
    val maxRate: Int = args(2).toInt // 1000
    val durations: Int = args(3).toInt // 60
    val prefix = "/data/radius/streaming/"
    def f = (sc: SparkContext, messages: DStream[RadiusMessage]) => {
      // Nếu không cache DStream, khi bạn thực hiện nhiềm dstream, bạn sẽ gặp file vấn đề: 
      //      KafkaConsumer is not safe for multi-threaded access
//      val dstream = message
//        .map(x => Parser.parse(x.message, DateTimeUtil.create(x.timeStamp / 1000).toString("yyyy-MM-dd")))
//        .cache()
      messages.foreachRDD(rdd => {
        
        //val sc = Configure.getSparkContext(es)
        val esInternal = new EsConnection("172.27.11.156", 9200, "radius-index", "docs")
        val log = rdd.map(x => Parser.parse(x.message, x.timeStamp)).cache()
        val load = log.filter(x => x.isInstanceOf[LoadLog]).map(x => x.asInstanceOf[LoadLog]).cache()
        val err = log.filter(x => x.isInstanceOf[ErrLog]).map(x => x.asInstanceOf[ErrLog]).cache()
        val con = log.filter(x => x.isInstanceOf[ConLog]).map(x => x.asInstanceOf[ConLog]).cache()
        //val time = DateTimeUtil.create(load.map(x => x.timestamp).reduce(Math.min) / 1000)
        val time = DateTimeUtil.now.minusSeconds(durations)
        load.saveAsTextFile(prefix + "/load/" + time.toString(DateTimeUtil.YMD + "/HH/mm/ss"))
        err.saveAsTextFile(prefix + "/err/" + time.toString(DateTimeUtil.YMD + "/HH/mm/ss"))
        con.saveAsTextFile(prefix + "/con/" + time.toString(DateTimeUtil.YMD + "/HH/mm/ss"))
        rdd.map(x => RawLog(x.timeStamp, x.message)).saveAsTextFile(prefix + "/raw/" + time.toString(DateTimeUtil.YMD + "/HH/mm/ss"))

        if (!local) {
          esInternal.save(load.filter(x => x!= null).map(x => x.toES()), s"radius-streaming-${time.toString("yyyy-MM-dd")}", "load")
          esInternal.save(err.filter(x => x!= null).map(x => x.toES()), s"radius-streaming-${time.toString("yyyy-MM-dd")}", "err")
          esInternal.save(con.filter(x => x!= null).map(x => x.toES()), s"radius-streaming-${time.toString("yyyy-MM-dd")}", "con")
          esInternal.save(rdd.filter(x => x!= null).map(x => RawLog(x.timeStamp, x.message)).map(x => x.toES()), s"radius-streaming-${time.toString("yyyy-MM-dd")}", "raw")
          //rdd.map(x => RawLog(x.timeStamp, x.message)).saveAsTextFile(prefix + "/raw/" + time.toString(DateTimeUtil.YMD + "/HH/mm/ss"))
        }
        // Write IP, Name and time for mapping contract in DNS
        if (!local) {
          val redisNodes = "172.27.11.173:6379,172.27.11.175:6379,172.27.11.176:6379,172.27.11.173:6380,172.27.11.175:6380,172.27.11.176:6380"
          load.foreachPartition(p => {
            val redisClient = RedisClusterClientFactory.getOrCreateClient(redisNodes)
            val kvs: Iterator[(String, String)] = p.map(x => {
              (x.ipAddress, x.name.toLowerCase() + "," + x.timestamp)
            })
            kvs.foreach(kv => {
              try {
                redisClient.lpush(kv._1, kv._2.toString)
                redisClient.ltrim(kv._1, 0, 60)
              } catch {
                case e: Exception => println("just simply ignore" + kv._1 + " ---- " + kv._2)
              }
            })
          })
        }
      })

      if (!local) {
        messages.foreachRDD(rdd => {
          rdd.map(x => x.topic + "-" + x.partition -> x.offset)
            .reduceByKey(Math.max)
            .foreach(x => {
              val r = new RedisClient(REDIS_HOST, REDIS_PORT)
              val topic = x._1.split("-")(0)
              val partition = x._1.split("-")(1)
              val offset = x._2.toLong
              r.hset(topic, partition, offset)
            })
        })
      }
    }
    
    val es = new EsConnection("172.27.11.156", 9200, "radius-index", "docs")
    val sparkConf = createSparkConf(maxRate)
    es.configure(sparkConf)
    
    val ssc = new StreamingContext(sparkConf, Seconds(durations))

    val offsetsString = if (!local) {
      val r = new RedisClient(REDIS_HOST, REDIS_PORT)
      val v = r.hgetall1(topic)
      if (v.isEmpty) null else v.get.toArray.map(x => x._1 + ":" + x._2).mkString(",")
    } else null
    println("OFFSET: " + offsetsString)
    KafkaStreaming.run(ssc, brokers, topic, offsetsString, maxRate, durations, f)
  }

  def createSparkConf(maxRate: Int): SparkConf = {
    val sparkConf = new SparkConf() //.setAppName("Kafka Driver").setMaster("local[1]")
    /**
     * Configure for Spark.
     */
    //sparkConf.set("spark.scheduler.mode", "FAIR");
    //sparkConf.set("spark.default.parallelism", "8");
    //sparkConf.set("spark.task.cpus", "4");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
    sparkConf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    sparkConf.set("spark.streaming.concurrentJobs", "4")
    sparkConf.set("spark.streaming.unpersist", "true")

    sparkConf.set("log4j.configuration", "org/apache/spark/log4j.properties");

    /**
     * Configure for Kafka
     */
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", maxRate.toString)

    //sparkConf.set("spark.driver.allowMultipleContexts", "true")

    /**
     * Config Elasticsearch
     */
    //sparkConf.set("es.resource", esIndex + "/" + esType)
    sparkConf.set("es.nodes", "172.27.11.156" + ":" + "9200") // List IP/Hostname/host:port
    //sparkConf.set("es.port", port)  // apply for host in es.nodes that do not have any port specified
    // For ES version 5.x, Using 'create' op will error if you don't set id when create
    // To automatic ID generate, using 'index' op
    sparkConf.set("es.write.operation", "index")
    sparkConf.set("es.batch.size.bytes", "10mb")
    sparkConf.set("es.batch.size.entries", Integer.toString(1000)) // default 1000
    sparkConf.set("es.batch.write.refresh", "true")
    sparkConf
  }

  @deprecated
  private def runLocalBK() {
    // Kafka Information
    val brokers: String = "172.27.11.75:9092,172.27.11.80:9092,172.27.11.85:9092"
    val topic: String = "radius"
    val offsetsString = null //readCheckPoint()
    val maxRate: Int = 1000
    val durations: Int = 60

    val prefix = "/data/radius/streaming/"
    //val log = args(5)
    def f = (sc: SparkContext, message: DStream[RadiusMessage]) => {
      //val timestamp = message.
      // Nếu không cache DStream, khi bạn thực hiện nhiềm dstream, bạn sẽ gặp file vấn đề: 
      //      KafkaConsumer is not safe for multi-threaded access
      val dstream = message
        .map(x => Parser.parse(x.message, x.timeStamp))
        .cache()

      dstream.foreachRDD(rdd => {
        //def f = (x: AbstractLog) => x.getKey
        //save(rdd, f, prefix)

        //def now = DateTimeUtil.now.minusSeconds(durations)
        val load = rdd.filter(x => x.isInstanceOf[LoadLog]).map(x => x.asInstanceOf[LoadLog])
        val err = rdd.filter(x => x.isInstanceOf[ErrLog]).map(x => x.asInstanceOf[ErrLog])
        val con = rdd.filter(x => x.isInstanceOf[ConLog]).map(x => x.asInstanceOf[ConLog])

        val time = DateTimeUtil.create(load.map(x => x.timestamp).reduce(Math.min) / 1000)

        load.saveAsTextFile(prefix + "/load/" + time.toString(DateTimeUtil.YMD + "/HH/mm/ss"))
        err.saveAsTextFile(prefix + "/err/" + time.toString(DateTimeUtil.YMD + "/HH/mm/ss"))
        con.saveAsTextFile(prefix + "/con/" + time.toString(DateTimeUtil.YMD + "/HH/mm/ss"))
      })
    }
    val sparkConf = createSparkConf(maxRate)
    val ssc = new StreamingContext(sparkConf, Seconds(durations))
    KafkaStreaming.run(ssc, brokers, topic, offsetsString, maxRate, durations, f)
  }
  
  
  
}