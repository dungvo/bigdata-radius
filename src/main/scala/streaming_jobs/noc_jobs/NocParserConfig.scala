package streaming_jobs.noc_jobs

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 06/07/2017.
  */
case class NocParserConfig(inputTopic: String,
                           streamingBatchDurations: FiniteDuration,
                           streamingCheckPointDir: String,
                           sparkConfig: Map[String,String],
                           souceKafka: Map[String,String]) extends Serializable{
  import com.typesafe.config.{Config, ConfigFactory}
}
object NocParserConfig {
  import net.ceedubs.ficus.Ficus._
  def apply() : NocParserConfig = apply(ConfigFactory.load)
  def apply(infConfig: Config):NocParserConfig = {
    val config = infConfig.getConfig("nocConfig")
    new NocParserConfig(
      config.as[String]("input.topic"),
      config.as[FiniteDuration]("streamingBatchDuration"),
      config.as[String]("streamingCheckPointDir"),
      config.as[Map[String,String]]("sparkConfig"),
      config.as[Map[String,String]]("sourceKafka")
    )
  }
}
