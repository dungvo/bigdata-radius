package streaming_jobs.inf_jobs

import java.sql.SQLException
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.count
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import storage.postgres.PostgresIO
import util.DatetimeController

import scala.collection.mutable.ListBuffer

/**
  * Created by hungdv on 25/09/2017.
  */
object LosOltDetect {
  def run(ssc: StreamingContext,
          ss: SparkSession,
          lines: DStream[String],
          postgresConfig: Map[String, String]): Unit = {
    val objectDStream: DStream[(String, String, String)] = lines.transform(reconstructObject)
    /* val moduleError = "module/cpe error"
     val portdownError = "user port down"
     val disconnectError = "disconnect/lost IP"*/
    val jdbcUrl = PostgresIO.getJDBCUrl(postgresConfig)

    //println("START INF JOB")
    val sc = ss.sparkContext
    val bJdbcURL = sc.broadcast(jdbcUrl)
    val pgProperties = new Properties()
    pgProperties.setProperty("driver", "org.postgresql.Driver")
    val bPgProperties = sc.broadcast(pgProperties)
    import ss.implicits._
    //TEST
    //objectDStream.window(Duration(60*1000*1),Duration(5*1000)).foreachRDD{  rdd =>
    //PROD
    objectDStream.window(Duration(120 * 1000 * 3), Duration(60 * 1000)).foreachRDD { (rdd, time: org.apache.spark.streaming.Time) =>
      val los = rdd.toDF("time","module","index")
      if(los.count() > 0) {
        los.show()
        val los_count_by_module = los.groupBy("module","time").agg(count("index").as("number_of_users_los"))
        val ids = los_count_by_module.select("module").rdd.map(r => r(0)).collect()
        var modulesString = "("
        ids.foreach { x =>
          val y = "'" + x + "',"
          modulesString = modulesString + y
        }
        modulesString = modulesString.dropRight(1) + " )"
        // TODO migrate to SQL
        // Get thres hold db for specific bras_ids
        // Cassandra version
        //val theshold = spark.sql(s"Select * from bras_theshold WHERE bras_id IN $brasIdsString").cache()
        // Postgres version:
        val index_count_by_module = s"( Select * from inf_index_count_by_module WHERE module_id IN $modulesString ) as bhm "
        //println("thresholdQuery " + thesholdQuery)
        val theshold = PostgresIO.pushDownQuery(ss, bJdbcURL.value,index_count_by_module, bPgProperties.value).toDF("module","theshold")
        val count_joined = theshold.join(los_count_by_module,"module")
        val candidates = count_joined.where("number_of_users_los > (theshold * 60 /100)")
        candidates.show()
        //Save To Postgres.
        try {
          PostgresIO.writeToPostgres(ss, candidates, bJdbcURL.value, "los_pattern", SaveMode.Append, bPgProperties.value)
        } catch {
          case e: SQLException => System.err.println("SQLException occur when save los_pattern : " + e.getSQLState + " " + e.getMessage)
          case e: Exception => System.err.println("UncatchException occur when save los_pattern : " + e.getMessage)
          case _ => println("Ignore !")
        }
      }

    }
  }

  def reconstructObject = (lines: RDD[String]) => lines.map {
    line =>
      val splited: Array[String] = line.split("#")
      if (splited.length >= 3) {
        val tuple = (splited(0), splited(1), splited(2))
        tuple
      } else ("n/a", "n/a", "n/a")

  }

}