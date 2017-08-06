package streaming_jobs.opsview_jobs

import core.sources.KafkaDStreamSource
import core.streaming.{SparkLogLevel, SparkStreamingApplication}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import streaming_jobs.inf_jobs.{InfParserConfig, ParseAndSaveInf}

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 31/07/2017.
  */
class ParseOpsviewJob (config: OpsviewConfig,source: KafkaDStreamSource)extends SparkStreamingApplication{
  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDurations

  override def streamingCheckpointDir: String = config.streamingCheckPointDir

  override def sparkConfig: Map[String, String] = config.sparkConfig

  def start(): Unit = {
    withSparkStreamingContext { (ss, ssc) =>
      val input: DStream[String] = source.createSource(ssc, config.inputTopic)
      val opsviewParser = new parser.OpsviewParser
      ParserAndSaveOpsview.parseAndSave(
        ssc,ss,input,opsviewParser,config.postgresConfig
      )
    }
  }
}
object OpsviewJob{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    SparkLogLevel.setStreamingLogLevels()
    val config = OpsviewConfig()
    val opsviewJob = new ParseOpsviewJob(config,KafkaDStreamSource(config.souceKafka))
    opsviewJob.start()
  }
}
