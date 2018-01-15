package streaming_jobs.dns

import core.sources.KafkaDStreamSource
import core.streaming.{SparkLogLevel, SparkStreamingApplication}
import org.apache.spark.streaming.dstream.DStream
import streaming_jobs.load_jobs.LoadJobConfig

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 31/10/2017.
  */
class DNSJob(config: DNSConfig,source: KafkaDStreamSource) extends SparkStreamingApplication {
  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDuration

  override def streamingCheckpointDir: String = config.streamingCheckPointDir
  // NOTICE: Predef.Map is not collection.Map !!!!
  override def sparkConfig: Predef.Map[String, String] = config.sparkConfig
  def start(): Unit = {
    withSparkStreamingContext{(sc,ssc) =>
      val input: DStream[String]  = source.createSource(ssc,config.inputTopic)
      val DSNParser = new DNSParser
      ParseAndMap.parserAndSave(ssc,sc,input,DSNParser,config.redisCluster,config.kafkaConfig,config.kafkaOutputTopic)
    }
  }
}

object DnsMapping{
  def main(args: Array[String]): Unit = {
    SparkLogLevel.setStreamingLogLevels()
    // Create new job config.
    val config = DNSConfig()
    // Create new Job
    val dnsMapping: DNSJob = new DNSJob(config,KafkaDStreamSource(config.sourceKafka))
    // Get the ball rolling.
    dnsMapping.start()
  }
}