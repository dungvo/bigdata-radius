package util

import java.sql.SQLException
import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}
import storage.postgres.PostgresIO

/**
  * Created by hungdv on 24/08/2017.
  */
object SparkStaticFileParsing {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("Parser bras ip mapping")
       .master("local[2]").getOrCreate()
    val sparkContext = sparkSession.sparkContext

    val raw = sparkContext.textFile("/home/hungdv/workspace/bigdata-radius/src/main/resources/bras_fpt_new.csv")
    val parsed = raw.map{ line =>
      val arr = line.split(",").map(_.trim)
      brasIpMapping(arr(2),arr(1))
    }
    import sparkSession.implicits._
    val df =parsed.toDF("bras_id","bras_ip")
    val postgresConfig = Map( "jdbcUsername" -> "dwh_noc",
    "jdbcPassword" -> "bigdata",
    "jdbcHostname" -> "172.27.11.153",
    "jdbcPort"     -> "5432",
    "jdbcDatabase" -> "dwh_noc",
    "jdbcHostnameForRead" -> "172.27.11.152",
    "jdbcDatabaseForRead" -> "big_data",
    "jdbcUsernameForRead" -> "big_data_query",
    "jdbcPasswordForRead" -> "bdquery")

    val jdbcUrl = PostgresIO.getJDBCUrl(postgresConfig)
    println(jdbcUrl)
    //FIXME :
    // Ad-hoc fixing
    val pgProperties    = new Properties()
    pgProperties.setProperty("driver","org.postgresql.Driver")

    try{
      PostgresIO.writeToPostgres(sparkSession, df, jdbcURL = jdbcUrl, "bras_name_ip_mapping", SaveMode.Overwrite, pgProperties)
    }catch{
      case e: SQLException => System.err.println("SQLException occur when save data : " + e.getSQLState + " " + e.getMessage)
      case e: Exception => System.err.println("UncatchException occur when save data : " +  e.getMessage)
      case _ => println("Ignore !")
    }


  }
}

case class brasIpMapping(bras_id: String,bras_ip: String) extends Serializable