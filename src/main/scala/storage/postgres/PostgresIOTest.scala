package storage.postgres
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, from_unixtime, udf, unix_timestamp}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import com.mongodb.spark.sql._
import org.joda.time.DateTime
/**
  * Created by hungdv on 29/04/2017.
  */
object PostgresIOTest {
  private val logger = Logger.getLogger(getClass)
  //private val master = "yarn"
  private val master = "yarn"
  private val appName = "Postgres"
  //val jdbcUsername = "postgres"
  //val jdbcUsername = "big_data_query"
  val jdbcUsername = "infdb"
  //val jdbcPassword = "hung"
  //val jdbcPassword = "bdquery"
  val jdbcPassword = "inf@db170120"

  val jdbcHostname = "172.27.11.9"
  val jdbcPort = 5432
  //val jdbcDatabase ="big_data"
  val jdbcDatabase ="inf_db"
  //val jdbcUrl = s"jdbc:postgresql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?stringtype=unspecified&rewriteBatchedStatements=true"
  val jdbcUrl = s"jdbc:postgresql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"
  //val connectionProperties = new java.util.Properties()

  def main(args: Array[String]): Unit = {
    // CREATE Table
    /* val TABLE_CONTRACT = "internet_contract"
      val  SQL_CREATE_TABLE_CONTRACT = "CREATE TABLE IF NOT EXISTS " + TABLE_CONTRACT + " (contract VARCHAR(9), object_id TEXT NOT NULL, name TEXT NOT NULL, profile TEXT, "+
      "upload_lim INT, download_lim INT, status TEXT NOT NULL, mac_address TEXT, " +
      "start_date TIMESTAMP, active_date TIMESTAMP, change_date TIMESTAMP, change_reason TEXT, "+
      "location TEXT, region TEXT, point_set TEXT, "+
      "host TEXT, port INT, slot INT, onu INT, cable_type TEXT, life_time INT, " + "PRIMARY KEY(contract));"
     println(SQL_CREATE_TABLE_CONTRACT)*/
    //
    val sparkSession = SparkSession.builder().appName(appName).master(master).getOrCreate()
    //sparkSession.conf.set("spark.driver.extraClassPath","/home/hungdv/workspace/postgresql-9.4.1212.jar")
    //val df           = loadEntireTable(sparkSession,jdbcUrl,"internet_contract")
    //df.show()
    val properties = new Properties()
    //properties.setProperty("user",jdbcUsername)
    //properties.setProperty("password",jdbcPassword)
    properties.setProperty("driver","org.postgresql.Driver")
    //properties.setProperty("driver","org.postgresql.Driver")
    //val df = PostgresIO.selectedByColumn(sparkSession,jdbcUrl,"internet_contract",List("name","host"),properties)
    //val df = PostgresIO.selectedByColumn(sparkSession,jdbcUrl,"internet_contract",List("name","host"),new Properties())
    //df.show()
    val schema = StructType(Array(StructField("bras_id", StringType, false),
      StructField("signin_total_count", IntegerType, false),
      StructField("logoff_total_count", IntegerType, false),
      StructField("reject_total_count", IntegerType, false),
      StructField("signin_distinct_count", IntegerType, false),
      StructField("logoff_distinct_count", IntegerType, false),
      StructField("reject_distinct_count", IntegerType, false),
      StructField("time", DateType, false)))
    //Apply Shema and read data to a dataframe
    val myDF = sparkSession.sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(schema)
      .load("/test/brascount.csv")
      //.load("src/main/resources/brascount.csv")
    //Show dataframe
    myDF.show()
    val timeFunc: (AnyRef => String) = (arg: AnyRef) => {
      getCurrentTime()
    }
    val sqlTimeFunc = udf(timeFunc)
    myDF.drop("time")

    val myDF2 = myDF.withColumn("time",sqlTimeFunc(col("bras_id")))
    myDF2.show()
    val myDF3 = myDF2.withColumn("time",org.apache.spark.sql.functions.current_timestamp())
    //val myDF3 = myDF2.withColumn("time",unix_timestamp(col("time"),"yyyy-MM-dd HH:mm:ss"))
    myDF3.show()
    PostgresIO.writeToPostgres(sparkSession,myDF3,jdbcUrl,"bras_count",SaveMode.Append,properties)

  }
  def getCurrentTime(): String ={
    val now: Date = Calendar.getInstance().getTime
    val nowFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //val nowFormeted: String = nowFormater.format(now)
    val nowFormeted = nowFormater.format(now).toString
    nowFormeted
    //now
    //val jodaDateTime = new DateTime(nowFormater)
    //val timeLong = jodaDateTime.getMillis
    //println(nowFormeted)
    //new Timestamp(timeLong)

  }
}