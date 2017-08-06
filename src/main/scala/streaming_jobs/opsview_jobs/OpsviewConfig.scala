package streaming_jobs.opsview_jobs

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 31/07/2017.
  */
case class OpsviewConfig(inputTopic: String,
                           streamingBatchDurations: FiniteDuration,
                           streamingCheckPointDir: String,
                           sparkConfig: Map[String,String],
                           souceKafka: Map[String,String],
                           postgresConfig: Map[String,String]) extends Serializable{
  import com.typesafe.config.{Config, ConfigFactory}
}

object OpsviewConfig {
  import net.ceedubs.ficus.Ficus._
  def apply() : OpsviewConfig = apply(ConfigFactory.load)
  def apply(infConfig: Config):OpsviewConfig = {
    val config = infConfig.getConfig("opsviewConfig")
    new OpsviewConfig(
      config.as[String]("input.topic"),
      config.as[FiniteDuration]("streamingBatchDuration"),
      config.as[String]("streamingCheckPointDir"),
      config.as[Map[String,String]]("sparkConfig"),
      config.as[Map[String,String]]("sourceKafka"),
      config.as[Map[String,String]]("postgresConfig")
    )
  }
}
