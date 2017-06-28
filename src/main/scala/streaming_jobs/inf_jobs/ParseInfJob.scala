package streaming_jobs.inf_jobs

import com.typesafe.config.{Config, ConfigFactory}
import core.sources.KafkaDStreamSource
import core.streaming.{SparkLogLevel, SparkStreamingApplication}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 19/06/2017.
  */
class ParseInfJob(config: InfParserConfig,source: KafkaDStreamSource)extends SparkStreamingApplication{
  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDurations

  override def streamingCheckpointDir: String = config.streamingCheckPointDir

  override def sparkConfig: Map[String, String] = config.sparkConfig

  def start(): Unit = {
    withSparkStreamingContext { (ss, ssc) =>
      val input: DStream[String] = source.createSource(ssc, config.inputTopic)
      val infParser = new parser.INFLogParser
      ParseAndSaveInf.parseAndSave(
        ssc,ss,input,infParser
      )
    }
  }
}

object InfJob{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    SparkLogLevel.setStreamingLogLevels()
    val config = InfParserConfig()
    val infJob = new ParseInfJob(config,KafkaDStreamSource(config.souceKafka))
    infJob.start()
  }
}