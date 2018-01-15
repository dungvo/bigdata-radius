package streaming_jobs.dns

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 31/10/2017.
  */
case class DNSConfig(inputTopic: String,streamingBatchDuration: FiniteDuration,
                     streamingCheckPointDir: String,
                     sparkConfig: Map[String,String],
                     sourceKafka: Map[String,String],
                     redisCluster: String,
                     kafkaConfig: Map[String,String],
                     kafkaOutputTopic: String) extends Serializable{

}
object DNSConfig{
  import com.typesafe.config.{Config,ConfigFactory}
  import net.ceedubs.ficus.Ficus._
  def apply(): DNSConfig = apply(ConfigFactory.load)

  def apply(dnsConfig: Config): DNSConfig = {
    val config = dnsConfig.getConfig("dsnConfig")
    new DNSConfig(
      config.as[String]("input.topic"),
      config.as[FiniteDuration]("streamingBatchDuration"),
      config.as[String]("streamingCheckPointDir"),
      //config.as[String]("streamingCheckPointDir"),
      config.as[Map[String,String]]("sparkConfig"),
      config.as[Map[String,String]]("sourceKafka"),
      config.as[String]("redis-cluster"),
      config.as[Map[String,String]]("sinkKafka"),
      config.as[String]("kafkaOutputTopic")
    )
  }
}