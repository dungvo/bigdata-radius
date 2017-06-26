package streaming_jobs.load_jobs

import core.sources.KafkaDStreamSource
import core.streaming.{SparkLogLevel, SparkStreamingApplication}
import org.apache.spark.streaming.dstream.DStream
import parser.LoadLogParser
import streaming_jobs.load_jobs.LoadJobConfig

import scala.concurrent.duration.FiniteDuration
/**
  * Created by hungdv on 09/05/2017.
  */
class LoadJob(config: LoadJobConfig,source: KafkaDStreamSource)extends SparkStreamingApplication{
  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDuration

  override def streamingCheckpointDir: String = config.streamingCheckPointDir
  // NOTICE: Predef.Map is not collection.Map !!!!
  override def sparkConfig: Predef.Map[String, String] = config.sparkConfig


  def start(): Unit ={
    withSparkStreamingContext{(ss,ssc) =>
      //val input: DStream[String]  = source.createSourceWithTimeStamp(ssc,config.inputTopic)
      val input: DStream[String]  = source.createSource(ssc,config.inputTopic)
      val loadlogParser = new LoadLogParser
      ParseAndSave.parserAndSave(
        ssc,
        ss,
        input,
        loadlogParser
      )
    }
  }
}
object LoadJob{
  def main(args: Array[String]): Unit = {
    // Set log level - defaul is [WARN]
    SparkLogLevel.setStreamingLogLevels()
    // Create new job config.
    val config = LoadJobConfig()
    // Create new Job
    val loadJob: LoadJob = new LoadJob(config,KafkaDStreamSource(config.sourceKafka))
    // Get the ball rolling.
    loadJob.start()
  }
}
