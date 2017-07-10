package streaming_jobs.noc_jobs

import com.esotericsoftware.minlog.Log.Logger
import core.sources.KafkaDStreamSource
import core.streaming.{SparkLogLevel, SparkStreamingApplication}
import org.apache.log4j.{Level, Logger}


import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 06/07/2017.
  */
class ParseNocJob(config: NocParserConfig,source: KafkaDStreamSource) extends SparkStreamingApplication{
  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDurations

  override def streamingCheckpointDir: String = config.streamingCheckPointDir

  override def sparkConfig: Map[String, String] = config.sparkConfig

  def start(): Unit ={
    withSparkStreamingContext{
      (ss,ssc) =>
        val input = source.createSource(ssc,config.inputTopic)
        val nocparser = new parser.NocParser
        ParseAndSaveNoc.parseAndSave(ssc,ss,input,nocparser)
    }
  }

}
object NocJob{
  def main(args: Array[String]): Unit = {
    org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF)
    org.apache.log4j.Logger.getLogger("akka").setLevel(Level.OFF)
    SparkLogLevel.setStreamingLogLevels()
    val config = NocParserConfig()
    val nocJob = new ParseNocJob(config,KafkaDStreamSource(config.souceKafka))
    nocJob.start()
  }
}

