package streaming_jobs.conn_jobs

import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import parser.ConnLogParser
import core.sources.KafkaDStreamSource
import storage.postgres.PostgresIO
import core.streaming.SparkStreamingApplication
import util.SparkCommon
import core.streaming.FualttoleranceSparkStreamingApplication

import scala.concurrent.duration.FiniteDuration
import core.streaming.{MapBroadcast, SparkLogLevel}

import scala.Predef.Map
import scala.collection.{Map, mutable}
/**
  * Created by hungdv on 20/03/2017.
  */
class ConnJob(config: ConnJobConfig, source: KafkaDStreamSource) extends SparkStreamingApplication {
  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDuration

  override def streamingCheckpointDir: String = config.streamingCheckPointDir
  // NOTICE: Predef.Map is not collection.Map !!!!
  override def sparkConfig: Predef.Map[String, String] = config.sparkConfig

  def start(): Unit = {
    withSparkStreamingContext{ (ss,ssc) =>
      //handle failtolorent here.

      // get PostgresConfig from Application Configs.
      val postgresConfig: Predef.Map[String, String] =  config.postgresStorage
      // get jdbc url.
      val jdbcUrl = PostgresIO.getJDBCUrlForRead(postgresConfig)
      //FIXME :
      // Ad-hoc fixing
      val pgProperties = new Properties()
      pgProperties.setProperty("driver","org.postgresql.Driver")

      //
      // Load name and INF host from database -> df
      val lookupDf  = PostgresIO.selectedByColumn(ss,jdbcUrl,"internet_contract",List("name","host"),pgProperties)
      // convert dataframe to RD
      val lookupRDD    = lookupDf.rdd.map(row => (row(0).toString,row(1).toString))
      // FIXME : Use SparkCommon.convertRDDToMap if this method produce error
      val lookupMap: mutable.Map[String, String] = SparkCommon.convertRDDToConcurrentHashMap[String,String](lookupRDD)
      //Broascast cache to worker.
      // WARN: collection.Map mutable.Map --> import collection.{mutable,Map}
      //val bLookUpCache: Broadcast[Map[String, String]] = ss.sparkContext.broadcast[Map[String,String]](lookupMap)
      val bLookUpCache: Broadcast[collection.Map[String, String]] = MapBroadcast.getInstance(ss.sparkContext,lookupMap)
      // Create Source - get value of kafka message and timestamp of msg.
      val input: DStream[String]  = source.createSourceWithTimeStamp(ssc,config.inputTopic)
      // Create Source - get value of kafka message only
      //val input: DStream[String]  = source.createSource(ssc,config.inputTopic)
      //cache to speed-up processing if action fails
      input.persist(StorageLevel.MEMORY_AND_DISK_SER)
      //input.persist(StorageLevel.MEMORY_ONLY_SER)
      val conlogParser = new ConnLogParser
      ParseAndCountConnLog.parseAndCount(
        ssc,
        ss,
        input,
        config.windowDuration,
        config.slideDuration,
        config.cassandraStorage,
        config.mongoStorage,
        conlogParser,
        bLookUpCache,
        postgresConfig,
        config.powerBIConfig,
        config.radiusAnomalyDetectionKafkaTopic,
        config.producerConfig
        //PowerBiconfig
      )
    }
  }
}

object ConnJob{
  def main(args: Array[String]): Unit = {
    // Set log level - defaul is [WARN]
    SparkLogLevel.setStreamingLogLevels()
    // Create new job config.
    val config = ConnJobConfig()
    // Create new Job
    val conJob = new ConnJob(config,KafkaDStreamSource(config.sourceKafka))
    // Get the ball rolling.
    conJob.start()
  }
}
