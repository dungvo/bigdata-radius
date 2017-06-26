package streaming_jobs.load_jobs

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 09/05/2017.
  */
case class LoadJobConfig(inputTopic: String,
                         streamingBatchDuration: FiniteDuration,
                         streamingCheckPointDir: String,
                         sparkConfig: Map[String,String],
                         sourceKafka: Map[String,String]
                      )extends Serializable{

}

object LoadJobConfig{
  import com.typesafe.config.{Config,ConfigFactory}
  import net.ceedubs.ficus.Ficus._
  def apply(): LoadJobConfig = apply(ConfigFactory.load)

  def apply(loadConfig: Config): LoadJobConfig = {
    val config = loadConfig.getConfig("loadConfig")
    new LoadJobConfig(
      config.as[String]("input.topic"),
      config.as[FiniteDuration]("streamingBatchDuration"),
      config.as[String]("streamingCheckPointDir"),
      //config.as[String]("streamingCheckPointDir"),
      config.as[Map[String,String]]("sparkConfig"),
      config.as[Map[String,String]]("sourceKafka")
    )
  }
}
