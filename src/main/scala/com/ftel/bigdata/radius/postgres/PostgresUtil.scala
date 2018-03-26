package com.ftel.bigdata.radius.postgres

import java.sql
import java.text.SimpleDateFormat
import java.util.Properties

import com.ftel.bigdata.radius.anomaly.ADStreaming.DATE_FORMAT
import com.ftel.bigdata.radius.postgres.Driver.ROOT_PATH
import com.ftel.bigdata.utils.DateTimeUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._

object PostgresUtil {
  def getURL() = {
    val host ="172.27.11.153"
    val port = "5432"
    val dbName = "dwh_noc"
    val userName = "dwh_noc"
    val password = "bigdata"
    s"jdbc:postgresql://${host}:${port}/${dbName}?user=${userName}&password=${password}"
  }

  def getProperties() = {
    val properties = new Properties()
    properties.setProperty("driver","org.postgresql.Driver")
    properties
  }

  def getFromPostgres(table: String): DataFrame = {
    val sql = SparkSession.builder().getOrCreate()
    val properties = PostgresUtil.getProperties()
    val url = PostgresUtil.getURL()

    sql.read.format("com.databricks.spark.jdbc")
      .jdbc(url, table, properties)
  }

  def writeBrasToPostgres(day: String): Unit = {
    val sql = SparkSession.builder().getOrCreate()

    val properties = PostgresUtil.getProperties()
    val url = PostgresUtil.getURL()
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
      .csv(s"$ROOT_PATH/stats-con/$day/bras")
      .write
      .mode(SaveMode.Append)
      .jdbc(url, table, properties)
  }

  def convertToTimestamp(dateTime: String): sql.Timestamp = {
    val formatter = new SimpleDateFormat(DATE_FORMAT)
    val utilDate = formatter.parse(dateTime)
    new sql.Timestamp(utilDate.getTime)
  }

  def getInformation(df: DataFrame, t: String) = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val prev30m = DateTimeUtil.create(t, DATE_FORMAT).minusMinutes(30).toString(DATE_FORMAT)

    val kibana_agg = PostgresUtil
      .getFromPostgres(s"(select * from dwh_kibana_agg where date_time between '$prev30m' and '$t') dwh_kibana_agg")
      .groupBy("bras_id").agg(sum($"crit_count").alias("crit_count"), sum($"err_count").alias("err_count"))


    val opsview = PostgresUtil
      .getFromPostgres(s"(select * from dwh_opsview_status where date_time between '$prev30m' and '$t') dwh_opsview_status")
      .groupBy("bras_id").agg(sum($"warn_opsview").alias("warn_opsview"), sum($"crit_opsview").alias("crit_opsview"))

    val infhost = PostgresUtil
      .getFromPostgres(s"(select * from dwh_inf_host where date_time between '$prev30m' and '$t') dwh_inf_host")
      .groupBy("bras_id").agg(sum($"cpe_error").alias("cpe_error"), sum($"lostip_error").alias("lostip_error"))

    val timeStamp = convertToTimestamp(t)

    df.select("bras_id", "signIn", "logOff", "active_users", "label")
      .join(kibana_agg, Seq("bras_id"), "leftouter")
      .join(opsview, Seq("bras_id"), "leftouter")
      .join(infhost, Seq("bras_id"), "leftouter")
      .na.fill(0L)
      .map{case row => (timeStamp, row.getString(0), row.getInt(3).toLong, row.getLong(1), row.getLong(2), row.getLong(9),
        row.getLong(10), row.getLong(5), row.getLong(6), row.getLong(7), row.getLong(8), row.getString(4))}
      .toDF("date_time", "bras_id", "active_users", "signIn", "logOff", "cpe_error", "lostip_error", "crit_count",
        "err_count", "warn_opsview", "crit_opsview", "label")
  }

  def writeOutlierToPostgres(df: DataFrame): Unit = {
    val sql = SparkSession.builder().getOrCreate()
    import sql.implicits._


    val properties = PostgresUtil.getProperties()
    val url = PostgresUtil.getURL()
    val table = "dwh_conn_bras_detail"


    df.write
      .mode(SaveMode.Append)
      .jdbc(url, table, properties)
  }

}
