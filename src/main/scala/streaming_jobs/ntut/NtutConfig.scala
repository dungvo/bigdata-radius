package streaming_jobs.ntut

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 05/01/2018.
  */
case class NtutConfig(inputTopic: String,
                      streamingBatchDuration: FiniteDuration,
                      streamingCheckpointDir: String,
                      sparkConfig: Map[String,String],
                      sourceKafka: Map[String,String])
  extends Serializable{
}

object NtutConfig{
  import com.typesafe.config.{Config,ConfigFactory}
  import net.ceedubs.ficus.Ficus._
  def apply(): NtutConfig = apply(ConfigFactory.load)
  def apply(appConfig: Config): NtutConfig ={
    val config = appConfig.getConfig("ntut")
    new NtutConfig(
      config.as[String]("input.topic"),
      config.as[FiniteDuration]("streamingBatchDuration"),
      config.as[String]("streamingCheckPointDir"),
      config.as[Map[String,String]]("sparkConfig"),
      config.as[Map[String,String]]("sourceKafka")
    )
  }
}
