package storage.postgres

import java.util.Properties
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.collection.JavaConverters._

/**
  * Created by hungdv on 28/04/2017.
  */
class PostgresIO {

}
// Fixme : Change to Singleton object.
object PostgresIO extends Serializable{
  private val logger = Logger.getLogger(getClass)
  import scala.language.implicitConversions
  def apply(): PostgresIO = new PostgresIO()
  /**
    *  ingesting an entire table -> Dataframe
    * @param sparkSession
    * @param jdbcURL  : jdbc URL Included host--port, database name- table name- user--pass
    * @param table    : table name
    * @return         : Dataframe containing content of table.
    */
  def loadEntireTable(sparkSession: SparkSession,jdbcURL: String,table: String): DataFrame={
    val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("url",jdbcURL)
      .option("dbtable",table)
      .load()
    jdbcDF
  }

  /**
    * ingesting an entire table -> Dataframe with specific properties
    * @param sparkSession
    * @param hostName
    * @param port
    * @param database
    * @param table
    * @param user
    * @param password
    * @return
    */
  def loadEntireTable(sparkSession: SparkSession,hostName: String,port: Int,database: String,
                      table: String,user: String,password: String): DataFrame={
    //val jdbcUrl = s"jdbc:mysql://${hostName}:${port}/${database}"
    // Can also use :
    val jdbcUrl = s"jdbc:postgresql://${hostName}:${port}/${database}?user=${user}&password=${password}"
    val df = loadEntireTable(sparkSession,jdbcUrl,table)
    df
  }

  /**
    * Pushdown a query to the database to leverage it for processing and only return the results.
    * @param sparkSession
    * @param jdbcUrl
    * @param jdbcQuery
    * @return
    */
  def pushDownQuery(sparkSession: SparkSession,jdbcUrl: String,jdbcQuery: String,connectionProperties: Properties)
  : DataFrame={
    val df = sparkSession.read.jdbc(jdbcUrl,jdbcQuery,connectionProperties)
    df
  }

  /**
    * Prune columns and just return the ones specified
    * @param sparkSession
    * @param jdbcUrl
    * @param tableName
    * @param columns
    * @param connectionProperties
    * @return
    */
  def selectedByColumn(sparkSession: SparkSession, jdbcUrl: String, tableName: String, columns: List[String],
                       connectionProperties: Properties): DataFrame={
    val dataFrameColumn = columns.map(column => org.apache.spark.sql.functions.column(column))
    val df = sparkSession.read.jdbc(jdbcUrl,table = tableName,connectionProperties).select(dataFrameColumn:_*)
    df
  }

  /**
    *
    * @param sparkSession
    * @param ds
    * @param jdbcURL
    * @param table
    * @param saveMode
    */
  def writeToPostgres(sparkSession: SparkSession,ds: DataFrame,jdbcURL: String,table: String, saveMode: SaveMode,connectionProperties: Properties): Unit={
    ds.write.mode(saveMode).jdbc(jdbcURL,table,connectionProperties)

    logger.info(s"${saveMode.toString} to $table successfully")
  }
  def writeToPostgres(sparkSession: SparkSession,ds: DataFrame, dbName: String,host: String,port: Int,userName: String,
                   password: String,table: String, saveMode: SaveMode,connectionProperties: Properties):Unit={
    val jdbcUrl = s"jdbc:postgresql://${host}:${port}/${dbName}?user=${userName}&password=${password}"
    writeToPostgres(sparkSession,ds,jdbcUrl,table,saveMode,connectionProperties)
  }
  def getJDBCUrl(postgresConfig: Map[String,String]): String ={
    val jdbcUsername = postgresConfig.getOrElse("jdbcUsername","big_data_query")
    val jdbcPassword = postgresConfig.getOrElse("jdbcPassword","bdquery")
    val jdbcHostname = postgresConfig.getOrElse("jdbcHostname","172.27.11.151")
    val jdbcPort     = postgresConfig.getOrElse("jdbcPort","5432").toInt
    val jdbcDatabase = postgresConfig.getOrElse("jdbcDatabase","big_data")
    val jdbcUrl      = s"jdbc:postgresql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"
    jdbcUrl
  }

  /**
    * Return JDBC URL for read.
    * @param postgresConfig
    * @return
    */
  def getJDBCUrlForRead(postgresConfig: Map[String,String]): String ={
    val jdbcUsername = postgresConfig.getOrElse("jdbcUsernameForRead","big_data_query")
    val jdbcPassword = postgresConfig.getOrElse("jdbcPasswordForRead","bdquery")
    val jdbcHostname = postgresConfig.getOrElse("jdbcHostnameForRead","172.27.11.152")
    val jdbcPort     = postgresConfig.getOrElse("jdbcPort","5432").toInt
    val jdbcDatabase = postgresConfig.getOrElse("jdbcDatabaseForRead","big_data")
    val jdbcUrl      = s"jdbc:postgresql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"
    jdbcUrl
  }

}

