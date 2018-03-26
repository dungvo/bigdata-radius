package com.ftel.bigdata.radius.postgres

import java.util.Properties

import com.ftel.bigdata.conf.Configure
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._

object Driver {
  val ROOT_PATH = "/data/radius"
  val LOCAL_PATH = "/home/thanhtm"

  def writeToPostgresLocal(sc: SparkContext): Unit = {
    val sql = SparkSession.builder().getOrCreate()
    val properties = new Properties()
    properties.setProperty("driver","org.postgresql.Driver")

    val host ="localhost"
    val port = "5432"
    val dbName = "dwh_noc"
    val userName = "postgres"
    val password = "1"
    val url = s"jdbc:postgresql://${host}:${port}/${dbName}?user=${userName}&password=${password}"
    val table = "dwh_conn_port"

    val schema = StructType(Array(
      StructField("date_time", TimestampType, false),
      StructField("bras_id", StringType, false),
      StructField("linecard", StringType, false),
      StructField("card", StringType, false),
      StructField("port", StringType, false),
      StructField("signin", IntegerType, false),
      StructField("logoff", IntegerType, false),
      StructField("signin_user", IntegerType, false),
      StructField("logoff_user", IntegerType, false)))

    sql.read.option("delimiter","\t").schema(schema)
      .csv(LOCAL_PATH)
      .write
      .mode(SaveMode.Append)
      .jdbc(url, table, properties)
  }

  def getFromPostgresLocal(sc: SparkContext): Unit = {
    val sql = SparkSession.builder().getOrCreate()
    val properties = new Properties()
    properties.setProperty("driver","org.postgresql.Driver")

    val host ="localhost"
    val port = "5432"
    val dbName = "dwh_noc"
    val userName = "postgres"
    val password = "1"
    val url = s"jdbc:postgresql://${host}:${port}/${dbName}?user=${userName}&password=${password}"
    val table = "dwh_conn_port"


    sql.read
      .jdbc(url, table, properties)
      .show(20)
  }



  def main(args: Array[String]): Unit = {
    val sc = Configure.getSparkContext()

    val table = "dwh_conn_port"
    val day = args(0)
    val flag = args(1)

    flag match {
      case "bras" => {
        PostgresUtil.writeBrasToPostgres(day)
        PostgresUtil.getFromPostgres(table)
      }
    }

  }

}
