package streaming_jobs.anomaly_detection

import core.sources.KafkaDStreamSource
import core.streaming.SparkStreamingApplication
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 14/06/2017.
  */
class IngestDataToCassandra(config: IngestConfig,source: KafkaDStreamSource)extends SparkStreamingApplication{
  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDuration

  override def streamingCheckpointDir: String = config.streamingCheckPointDir

  override def sparkConfig: Map[String, String] = config.sparkConfig

  def ingest(): Unit ={
    withSparkStreamingContext {
      (sc, ssc) =>
        val input: DStream[String] = source.createSource(ssc,config.inputKafkaTopic)
        ParseAndSave.parseAndSave(
          ssc,sc,input,config.cassandraConfig
        )
    }
  }
}
object IngestExe{
  def main(args: Array[String]): Unit = {
    val config = IngestConfig()
    val ingestDataToCassandra = new IngestDataToCassandra(config,KafkaDStreamSource(config.sourceKafka))
    ingestDataToCassandra.ingest()
  }
}
