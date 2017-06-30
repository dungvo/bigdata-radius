package streaming_jobs.anomaly_detection

import core.sources.KafkaDStreamSource
import core.streaming.SparkStreamingApplication
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by hungdv on 11/06/2017.
  */
class PointDetect(config: PointDetectConfig,source: KafkaDStreamSource) extends SparkStreamingApplication{
  override def streamingBatchDuration = config.streamingBatchDuration

  override def streamingCheckpointDir = config.streamingCheckPointDir

  override def sparkConfig = config.sparkConfig

  def detect(): Unit = {
    withSparkStreamingContext{
      (sc,ssc) =>
        val input: DStream[String] = source.createSource(ssc,config.inputTopic)
        val createDDL: String =
          """CREATE TEMPORARY VIEW brasscount
             USING org.apache.spark.sql.cassandra
             OPTIONS (
             table "brasscount",
             keyspace "radius",
             pushdown "true")"""

        val createDLLBrasThreshold  =
          """CREATE TEMPORARY VIEW bras_theshold
             USING org.apache.spark.sql.cassandra
             OPTIONS (
             table "bras_theshold",
             keyspace "radius",
             pushdown "true")"""
        val spark = sc.sqlContext
        spark.sql(createDDL)
        spark.sql(createDLLBrasThreshold)
    /*    DetectAnomaly.detect(ssc,
          sc,
          input,
          config.windowDuration,
          config.slideDuration,
          config.inputTopic,
          config.powerBIConfig
        )*/
        DetectAnomalyVer2.detect(ssc,
          sc,
          input,
          //config.windowDuration,
          //config.slideDuration,
          config.inputTopic,
          config.powerBIConfig
        )
    }
  }

}
object PointDetecExe{
  def main(args: Array[String]): Unit = {
    core.streaming.SparkLogLevel.setStreamingLogLevels()

    val config = PointDetectConfig()
    val pointDetect = new PointDetect(config,KafkaDStreamSource(config.sourceKafka))
    pointDetect.detect()
  }
}

