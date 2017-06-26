package batch_jobs

/**
  * Created by hungdv on 31/05/2017.
  */
case class ImportFromESToPostgresConfig(
                                        esConfig: Map[String,String],
                                        postgresConfig: Map[String,String],
                                        sparkConfig: Map[String,String]
                                       ) extends Serializable {
}
object ImportFromESToPostgresConfig {

  import com.typesafe.config.{Config, ConfigFactory}
  import net.ceedubs.ficus.Ficus._

  def apply(): ImportFromESToPostgresConfig = apply(ConfigFactory.load())

  def apply(importPostgresConfig: Config): ImportFromESToPostgresConfig = {
        val config = importPostgresConfig.getConfig("importPostgresConfig")
         new ImportFromESToPostgresConfig(
           config.as[Map[String,String]]("esConfig"),
           config.as[Map[String,String]]("postgresConfig"),
           config.as[Map[String,String]]("sparkConfig")
         )
  }
}
