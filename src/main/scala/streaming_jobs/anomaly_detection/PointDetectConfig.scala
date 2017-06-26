package streaming_jobs.anomaly_detection

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 11/06/2017.
  */
case class PointDetectConfig(
                            inputTopic: String,
                            windowDuration: FiniteDuration,
                            slideDuration: FiniteDuration,
                            streamingBatchDuration: FiniteDuration,
                            streamingCheckPointDir: String,
                            sparkConfig: Map[String,String],
                            sourceKafka: Map[String,String],
                            powerBIConfig: Map[String,String]
                            )extends Serializable{

}

object PointDetectConfig {
  import com.typesafe.config.{Config,ConfigFactory}
  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader.arbitraryTypeValueReader

  def apply(): PointDetectConfig = apply(ConfigFactory.load)

  def apply(prefilterConfig: Config): PointDetectConfig = {
    val config = prefilterConfig.getConfig("anomalyConfig")
    new PointDetectConfig(
      config.as[String]("inputTopic"),
      config.as[FiniteDuration]("windowDuration"),
      config.as[FiniteDuration]("slideDuration"),
      config.as[FiniteDuration]("streamingBatchDuration"),
      config.as[String]("streamingCheckPointDir"),
      config.as[Predef.Map[String,String]]("sparkConfig"),
      config.as[Predef.Map[String,String]]("sourceKafka"),
      config.as[Predef.Map[String,String]]("powerBIConfig")

    )
  }
}
