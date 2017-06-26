package streaming_jobs.anomaly_detection

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 14/06/2017.
  */
case class IngestConfig(
                       inputKafkaTopic: String,
                       sparkConfig: Map[String,String],
                       streamingBatchDuration: FiniteDuration,
                       streamingCheckPointDir: String,
                       sourceKafka: Map[String,String],
                       cassandraConfig: Map[String,String]

                       )extends Serializable{}
object IngestConfig {
  import com.typesafe.config.{Config,ConfigFactory}
  import net.ceedubs.ficus.Ficus._
  def apply():IngestConfig = apply(ConfigFactory.load)

  def apply(ingestConfig: Config): IngestConfig = {
    val config = ingestConfig.getConfig("ingestConfig")
    new IngestConfig(
      config.as[String]("inputKafkaTopic"),
      config.as[Map[String,String]]("sparkConfig"),
      config.as[FiniteDuration]("streamingBatchDuration"),
      config.as[String]("streamingCheckPointDir"),
      config.as[Map[String,String]]("sourceKafka"),
      config.as[Map[String,String]]("cassandraStorage")
    )
  }
}
