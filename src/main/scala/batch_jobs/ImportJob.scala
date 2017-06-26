package batch_jobs


import java.util.{Calendar, Properties}
import org.elasticsearch.spark._
import org.apache.commons.cli._
import core.streaming.SparkApplication
import storage.postgres.PostgresIO
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.elasticsearch.spark.sql._
/**
  * Created by hungdv on 31/05/2017.
  */
class ImportJob(config: ImportFromESToPostgresConfig) extends SparkApplication {

  override def sparkConfig: Map[String,String] = config.sparkConfig

  def BRAS_PREFIX =  config.esConfig.getOrElse("bras-prefix","bras")
  def INF_PREFIX =  config.esConfig.getOrElse("inf-prefix","inf")
  def postgresConfig = config.postgresConfig


  def start(pToday : DateTime,pHour :Int): Unit ={
    withSparkSession{sparkSession =>
      // Config Variables.

      val pgProperties    = new Properties()
      pgProperties.setProperty("driver","org.postgresql.Driver")
      val jdbcURL = PostgresIO.getJDBCUrl(postgresConfig)

      val rdd = sparkSession.sparkContext.esRDD("count_by_bras-2017-05-31-15/bras_count","")
      val taken = rdd.take(10)
      taken.foreach(println(_))

      /*val bJdbcURL = sparkSession.sparkContext.broadcast(jdbcURL)
      // Read data from Es
      val brasCount = readDataFromES(sparkSession,BRAS_PREFIX,pToday,pHour)
      // Bras Count
      if(brasCount.count() > 0) {
        PostgresIO.writeToPostgres(sparkSession, brasCount, bJdbcURL.value, "bras_count", SaveMode.Append, pgProperties)
      }
      // Bras sum Count ??
      // Inf
      val infCount = readDataFromES(sparkSession,INF_PREFIX,pToday,pHour)
      // Save data to Postgres
      if(brasCount.count() > 0) {
        PostgresIO.writeToPostgres(sparkSession, infCount, bJdbcURL.value, "inf_count", SaveMode.Append, pgProperties)
      }*/

    }
  }

  def readDataFromES(sc : SparkSession,indexType: String): DataFrame ={
    val dataframe = sc.sqlContext.read
      //.format("es")
      .format("org.elasticsearch.spark.sql")
      .load("count_by_bras-2017-05-31-15/bras_count")
      //.load(resolveIndexAndType(indexType))
    dataframe
  }
  def readDataFromES(sc : SparkSession,indexType: String,pToday :DateTime,pHour :Int): DataFrame ={
    import org.elasticsearch.spark.sql._
    val dataframe = sc.sqlContext.read
      //.format("es")
      .format("org.elasticsearch.spark.sql")
      .load("count_by_bras-2017-05-31-15/bras_count")
      //.load(resolveIndexAndType(indexType,pToday,pHour))
    dataframe
  }
  def readDataFromESRDD(sc : SparkSession,indexType: String,pToday :DateTime,pHour :Int): DataFrame ={
    val dataframe = sc.sqlContext.read
      //.format("es")
      .format("org.elasticsearch.spark.sql")
      .load("count_by_bras-2017-05-31-15/bras_count")
      //.load(resolveIndexAndType(indexType,pToday,pHour))
    dataframe

  }
  def resolveIndexAndType(indexType: String,
                          pToday: DateTime = DateTime.now(),
                          pHour : Int = Calendar.getInstance().get(Calendar.HOUR_OF_DAY)): String ={

    var today = pToday
    var hour = pHour
    if(hour == 0 ) {
      today = today.minusDays(1)
      hour  = 24
    }

    val s_hour = (hour - 1)
    val todayString = today.toString("yyyy-MM-dd")

    val index = "count_by_" + indexType + "-" + todayString + "-"  + s_hour
    val _type  = indexType + "_count"
    val resul  = index + "/" + _type
    resul
  }

}

object ImportJobExe{
  var today = DateTime.now()
  var hour  :Int = Calendar.getInstance().get(Calendar.HOUR_OF_DAY)

  def main(args : Array[String]): Unit = {
    optionParser(args)
    val config = ImportFromESToPostgresConfig()
    val importJob = new ImportJob(config)

    importJob.start(today,hour)

  }
  def optionParser(args: Array[String]): Unit ={
    val options : Options = new Options()

    val date = new Option("d","date",false,"date to process - informat yyyy-MM-dd")
    date.setRequired(false)

    options.addOption(date)

    val time = new Option("t","time",false,"time to process")
    time.setRequired(false)

    options.addOption(time)
    val parser: CommandLineParser = new GnuParser()
    val formatter: HelpFormatter = new HelpFormatter()
    var cmd :CommandLine = null
    try
    {
      cmd = parser.parse(options, args)
    }
    catch
    {
      case e: ParseException => {
        System.out.println(e.getMessage())
        formatter.printHelp("utility-name", options)
        System.exit(1); return
      }
      case _: Throwable => println("Got some other kind of exception")
    }

    val todayString = cmd.getOptionValue("date","null")
    val hourString  = cmd.getOptionValue("time","null")

    if(todayString != "null"){
     //Convert String to joda Datetime
      today = convertStringToDatetime(todayString,"yyyy-MM-dd")
    }
    if(hourString != "null" ){
      //Check if hourString is numeric or not
    //if(hourString != "null" && isNumericPattern(hourString)){
    //Convert String to Int.
      hour = hourString.toInt
    }

  }
  def convertStringToDatetime(date: String, pattern: String): DateTime={
    val formater: DateTimeFormatter = DateTimeFormat.forPattern(pattern)
    val datetime = formater.parseDateTime(date)
    datetime
  }
  def isNumericPattern(s:String) :Boolean = s.matches("""[+-]?((\d+(e\d+)?[lL]?)|(((\d+(\.\d*)?)|(\.\d+))(e\d+)?[fF]?))""")
}
