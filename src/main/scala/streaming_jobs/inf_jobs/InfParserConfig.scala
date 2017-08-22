package streaming_jobs.inf_jobs

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 19/06/2017.
  */
case class InfParserConfig(inputTopic: String,
                      streamingBatchDurations: FiniteDuration,
                      streamingCheckPointDir: String,
                      sparkConfig: Map[String,String],
                      souceKafka: Map[String,String],
                           postgresConfig: Map[String,String],
                           infPortDownKafkaTopic: String,
                           producerConfig: Predef.Map[String,String]
                          ) extends Serializable{
import com.typesafe.config.{Config, ConfigFactory}
}

object InfParserConfig {
  import net.ceedubs.ficus.Ficus._
  def apply() : InfParserConfig = apply(ConfigFactory.load)
  def apply(infConfig: Config):InfParserConfig = {
    val config = infConfig.getConfig("infConfig")
    new InfParserConfig(
      config.as[String]("input.topic"),
      config.as[FiniteDuration]("streamingBatchDuration"),
      config.as[String]("streamingCheckPointDir"),
      config.as[Map[String,String]]("sparkConfig"),
      config.as[Map[String,String]]("sourceKafka"),
      config.as[Map[String,String]]("postgresConfig"),
      config.as[String]("infPortDownKafkaTopic"),
      config.as[Map[String,String]]("producerConfig")
    )
  }
}

