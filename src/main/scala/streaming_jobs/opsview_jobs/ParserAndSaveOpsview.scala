package streaming_jobs.opsview_jobs

import java.sql.SQLException
import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods.parse
import parser.{OpsviewLogLineObject, OpsviewParser}
import core.streaming.OpsParserBroadcast
import storage.postgres.PostgresIO
import org.apache.spark.sql.functions._

/**
  * Created by hungdv on 31/07/2017.
  */
object ParserAndSaveOpsview {
  def parseAndSave(ssc: StreamingContext,
                   ss: SparkSession,
                   kafkaMessages: DStream[String],
                   parser: OpsviewParser,
                   postgresConfig: Map[String,String]): Unit = {
    val sc = ss.sparkContext
    val bParser = OpsParserBroadcast.getInstance(sc,parser)
    val bStatusLevel = sc.broadcast(Seq("WARNING","UNKNOWN","CRITICAL","OK"))
    val jdbcUrl = PostgresIO.getJDBCUrl(postgresConfig)

    println(jdbcUrl)
    val bJdbcURL = sc.broadcast(jdbcUrl)
    val pgProperties    = new Properties()
    pgProperties.setProperty("driver","org.postgresql.Driver")
    val bPgProperties   = sc.broadcast(pgProperties)
    import ss.implicits._

    val lines = kafkaMessages.transform(extractMessageAndValue("message",bParser)).cache()
    lines.foreachRDD{
      rdd =>
        import org.apache.spark.sql.functions.unix_timestamp
        val ts = unix_timestamp($"date_time", "yyyy-MM-dd HH:mm:ss").cast("timestamp")

        val opsview_df = rdd.toDF("found_result","date_time","bras_id","service_name","service_status","alert_state","alert_code","message")
            .select("date_time","bras_id","service_name","service_status","message")
            .withColumn("date_time",ts)
            .cache()

        println("opsviewdf : " + opsview_df.count())

        try{
          PostgresIO.writeToPostgres(ss,opsview_df, bJdbcURL.value,"dwh_opsview",SaveMode.Append,bPgProperties.value)
        }catch{
          case e: SQLException => System.err.println("SQLException occur when save dwh_opsview : " + e.getSQLState + " " + e.getMessage)
          case e: Exception => System.err.println("UncatchException occur when save dwh_opsview : " +  e.getMessage)
          case _ => println("Ignore !")
        }


        //val opsview_df_ts = opsview_df.withColumn("timestamp",ts).drop("date_time")
       // val opsview_df_ts = opsview_df.withColumn("date_time",ts)
        val opsview_count = opsview_df.select("date_time","bras_id","service_status")
            .groupBy("date_time","bras_id","service_status").agg(count(col("service_status")).as("count_service_status"))

        val opsview_count_pivot =     opsview_count.groupBy("bras_id","date_time").pivot("service_status",bStatusLevel.value)
          .agg(expr("coalesce(first(count_service_status),0)"))
          .withColumnRenamed("WARNING","warn_opsview")
          .withColumnRenamed("UNKNOWN","unknown_opsview")
          .withColumnRenamed("CRITICAL","crit_opsview")
          .withColumnRenamed("OK","ok_opsview")
          //.withColumnRenamed("host_name","bras_id")
          .withColumnRenamed("timestamp","date_time")
        try{
          PostgresIO.writeToPostgres(ss,opsview_count_pivot, bJdbcURL.value,"dwh_opsview_status",SaveMode.Append,bPgProperties.value)
        }catch{
          case e: SQLException => System.err.println("SQLException occur when save dwh_opsview_status : " + e.getSQLState + " " + e.getMessage)
          case e: Exception => System.err.println("UncatchException occur when save dwh_opsview_status : " +  e.getMessage)
          case _ => println("Ignore !")
        }



        opsview_df.unpersist()
    }
  }

  def jsonStringToMap(jsonStr: String) : Map[String,Any] = {
    implicit val formats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, Any]]
  }
  def extractMessageAndValue = (key: String, bParser: Broadcast[OpsviewParser]) => (mesgs: RDD[String]) => mesgs.map { msg =>
    implicit val formats = org.json4s.DefaultFormats
    val value = parse(msg).extract[Map[String, Any]].get(key).getOrElse(null)
    value.asInstanceOf[String]
  }.filter(value => value != null).map { line =>
    val parserObject = bParser.value.extracValues(line.replace("\n", "").replace("\r", "")).getOrElse(None)
    parserObject match {
      case Some(x) => x
      case _ => None
    }
    parserObject
    // NOTICE !!!!!!
    //println("1-"+line.replaceAll("^\\s|\n\\s|\\s$", "") + "-END")
    //println("2-"+line.trim().replaceAll("\n ", "") + "-END")
    //println("3-"+line.replace("\n", "").replace("\r", "") + "-END")
  }.filter(x => x != None).map(ob => ob.asInstanceOf[OpsviewLogLineObject]).filter(x => x.host_name !="n/a")
}
