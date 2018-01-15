package streaming_jobs.inf_jobs

import java.sql.SQLException
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.count
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import storage.postgres.PostgresIO
import util.DatetimeController
import org.apache.spark.sql.functions._

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

    println("START INF DETECT LOS JOB")
    val sc = ss.sparkContext
    val bJdbcURL = sc.broadcast(jdbcUrl)
    val pgProperties = new Properties()
    pgProperties.setProperty("driver", "org.postgresql.Driver")
    val bPgProperties = sc.broadcast(pgProperties)
    import ss.implicits._
    //TEST
    //objectDStream.window(Duration(60*1000*1),Duration(5*1000)).foreachRDD{  rdd =>
      objectDStream.foreachRDD{  rdd =>
    //PROD
    //objectDStream.window(Duration(120 * 1000 * 3), Duration(60 * 1000)).foreachRDD { (rdd, time: org.apache.spark.streaming.Time) =>
      val los = rdd.toDF("time","module","index").cache
      los.createOrReplaceTempView("los")
      if(los.count() > 0) {
        println("los : " + los.show())
        val los_count_by_module = los.groupBy("module","time").agg(count("index").as("number_of_users_los"))
        val modulesString = getListValues(los_count_by_module,"module")
        val index_count_by_module = s"( Select * from inf_index_count_by_module WHERE module_id IN $modulesString ) as bhm "
        //println("thresholdQuery " + thesholdQuery)
        val theshold = PostgresIO.pushDownQuery(ss, bJdbcURL.value,index_count_by_module, bPgProperties.value).toDF("module","theshold")
        val count_joined = theshold.join(los_count_by_module,"module")
        //println("count joined")
        count_joined.show()
        val candidates = count_joined.where("number_of_users_los > (theshold * 60 /100 )  ").withColumn("server_time",org.apache.spark.sql.functions.current_timestamp())
        //println("candidate")
        candidates.show()
        //Save To Postgres.
        try {
          PostgresIO.writeToPostgres(ss, candidates, bJdbcURL.value, "los_pattern", SaveMode.Append, bPgProperties.value)
        } catch {
          case e: SQLException => System.err.println("SQLException occur when save los_pattern : " + e.getSQLState + " " + e.getMessage)
          case e: Exception => System.err.println("UncatchException occur when save los_pattern : " + e.getMessage)
          case _ => println("Ignore !")
        }

        val los_index = ss.sql("select concat(module,'/',index ) as index_id from los ")

        val indexs_string = getListValues(los_index,"index_id")
        val select_bc_by_indexs = s"( SELECT * FROM inf_bc_index where index_id IN $indexs_string ) as nmb "
        //println(select_bc_by_indexs)
        //println()
        val bc_index = PostgresIO.pushDownQuery(ss, bJdbcURL.value,select_bc_by_indexs, bPgProperties.value).toDF("bc_id","index_id")
        //bc_index.show()
        val index_count_by_bc = bc_index.groupBy("bc_id").agg(countDistinct("index_id").as("number_of_user_los")).cache()
        val bcs_string = getListValues(index_count_by_bc,"bc_id")
        val count_bcs: Long = index_count_by_bc.count()
        //println("count bcs " + count_bcs)
        if(count_bcs > 0){
          // dont remember why it is 5 :)) It's hard code man.
          val x = if(count_bcs < 5) "bc2" else  "olt"
          val select_threshold = s"( select * from inf_index_count_by_bc where bc_id IN $bcs_string ) as ntm "
          val bcs_threshold =  PostgresIO.pushDownQuery(ss, bJdbcURL.value,select_threshold, bPgProperties.value).toDF("bc_id","num_customer")
          val bcs_count_joined = index_count_by_bc.join(bcs_threshold,"bc_id")
          val bcs_candidates = bcs_count_joined.where(" (number_of_user_los > (num_customer * 60 / 100 )) AND (num_customer > 1 )")
            .withColumn("server_time",org.apache.spark.sql.functions.current_timestamp())
            .withColumn("devide_level",lit(x))
          bcs_candidates.show(100)
          try {
            PostgresIO.writeToPostgres(ss, bcs_candidates, bJdbcURL.value, "los_index_pattern", SaveMode.Append, bPgProperties.value)
          } catch {
            case e: SQLException => System.err.println("SQLException occur when save los_index_pattern : " + e.getSQLState + " " + e.getMessage)
            case e: Exception => System.err.println("UncatchException occur when save los_index_pattern : " + e.getMessage)
            case _ => println("Ignore !")
          }
        }
      }
    }
  }

  def getListValues(dataframe: DataFrame, columnName: String): String = {
    val bcs = dataframe.select(columnName).rdd.map(r => r(0)).collect()
    var bcs_string = "("
    bcs.foreach{x =>
      val y = "'" + x + "',"
      bcs_string = bcs_string + y
    }
    bcs_string = bcs_string.dropRight(1) + " )"
    bcs_string
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