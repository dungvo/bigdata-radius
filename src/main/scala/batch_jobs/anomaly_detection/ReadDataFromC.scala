package batch_jobs.anomaly_detection

import java.sql.Timestamp
import java.util
import java.util.TimerTask
import core.udafs.{MovingMedian, UpperIQR}
import org.apache.spark.sql.functions.{col, lit, rank, when}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import scala.concurrent.duration.FiniteDuration
import scalaj.http.{Http, HttpOptions}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.joda.time.DateTime

/**
  * Created by hungdv on 13/06/2017.
  */

object ReadDataFromC {
  val appName = "Radius-Anomaly-read_data_fromCas"
  val master = "local[2]" // Local dev only
  def logger = Logger.getLogger(getClass)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    conf
      .set("spark.cassandra.connection.host", "172.27.11.156")
      //.set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.output.batch.size.rows", "auto")
      .set("spark.master", "local[2]")
      //.set("spark.master", "yarn")
      .set("spark.app.name", "anomaly-detection-Radius")
      // mongo config
      //"spark.mongodb.output.uri" : "mongodb://172.27.11.146:27017/radius.conn_counting"
      //"spark.mongodb.output.uri" : "mongodb://localhost:27017/radius.conn_counting"
      //es test
      .set("es.port", "9200")
      .set("es.nodes","172.27.11.156")
      .set("es.http.timeout","5m")
      .set("es.scroll.size","50")
      .set("es.index.auto.create","true")
    val sparkSession = SparkSession.builder()
      //.appName(appName).master(master)
      .config(conf).getOrCreate()

    val sc = sparkSession.sparkContext
    val spark = sparkSession.sqlContext
    //Using cassandra RDD
    import com.datastax.spark.connector._
    //val cassRDD= sparkSession.sparkContext.cassandraTable("test","words").where("count > ?",5).foreach(println(_))
    //Using CassandraDF
    val powerBIConfig =
      Map("radius_anomaly_point_detect_url" -> "https://api.powerbi.com/beta/4ebc9261-871a-44c5-93a5-60eb590917cd/datasets/25f3994c-4044-4c97-b586-eab0dec67598/rows?key=pv02o%2FFyA%2BiZ8JvWt2Mj8Tm3WnYFl5VWpGKDm87fOPzbk4KtKouWAHixc3cXWBME7i8amvTEq3WvWggdDEdR9A%3D%3D",
          "proxy_host" -> "172.30.45.220")
    val bPowerBIConfig = sc.broadcast[Map[String, String]](powerBIConfig)
    val bPowerBIURL = sc.broadcast(powerBIConfig("radius_anomaly_point_detect_url"))
    val createDDL: String =
      """CREATE TEMPORARY VIEW brasscount
     USING org.apache.spark.sql.cassandra
     OPTIONS (
     table "brasscount",
     keyspace "radius",
     pushdown "true")"""
    spark.sql(createDDL) // Creates Catalog Entry registering an existing Cassandra Table
    // USE while true in case timer or executor not work
/*        while(true) {
          read(sparkSession,bPowerBIConfig)
          println("After - " + System.currentTimeMillis())
          Thread.sleep(60000)
          println("Befor - " +System.currentTimeMillis())
     }*/
    val timer = new java.util.Timer()
    timer.schedule(new doJob(sparkSession, bPowerBIConfig), 0, 90000)
  }
  def read(sparkSession: SparkSession, powerBIConfig: Broadcast[Map[String, String]]): Unit = {
    import sparkSession.implicits._
    val sc = sparkSession.sparkContext

    def spark = sparkSession.sqlContext

    val now = System.currentTimeMillis()
    val timestamp = new org.joda.time.DateTime(now).minusMinutes(16).toString("yyyy-MM-dd HH:mm:ss.SSS")

    val brasCounDF = spark.sql(s"SELECT * FROM brasscount WHERE time > '$timestamp'")
    println()
    println("COUNT :" + brasCounDF.count())
    println("TIME :" + timestamp)
    val rated = brasCounDF.withColumn("rateSL", ($"signin_total_count") / ($"logoff_total_count" + 1))
      .withColumn("rateLS", ($"logoff_total_count") / ($"signin_total_count" + 1))

    val upperBound = new UpperIQR
    val winMed = new MovingMedian
    val window = Window.partitionBy("bras_id").orderBy($"time").rowsBetween(-4, 0)
    val window2 = Window.partitionBy("bras_id").orderBy($"time")
    val window3 = Window.partitionBy("bras_id").orderBy($"time".desc)
    val movingMedian = rated.withColumn("moving_medianSL", winMed(col("rateSL")).over(window))
      .withColumn("moving_medianLS", winMed(col("rateLS")).over(window))
    val detrend = movingMedian.withColumn("detrendSL", ($"rateSL" - $"moving_medianSL"))
      .withColumn("detrendLS", ($"rateLS" - $"moving_medianLS"))
    //TODO Consider to do moving median again.
    val iqr = detrend.withColumn("upper_iqr_SL", upperBound(col("detrendSL")).over(window))
      .withColumn("upper_iqr_LS", upperBound(col("detrendLS")).over(window))

    val result: DataFrame = iqr.withColumn("outlier", when(($"detrendSL" > $"upper_iqr_SL" && $"upper_iqr_SL" > lit(0))
      || ($"detrendLS" > $"upper_iqr_LS" && $"upper_iqr_LS" > lit(0)), 1).otherwise(0))
      .withColumn("time_ranking", rank().over(window3))
    /*val result = iqr.withColumn("outlier",when(($"detrendSL" > $"upper_iqr_SL" )
                           || ($"detrendLS" > $"upper_iqr_LS" ),1).otherwise(0))*/
    //result.show()
    import org.elasticsearch.spark.sql._
    //result.saveToES("")
    val result2: DataFrame = result.select("bras_id", "signin_total_count", "logoff_total_count", "rateSL", "rateLS", "time")
      //.where($"outlier" > lit(0))
      //TODO uncomment this on  production mode
      .where($"outlier" > lit(0) && col("time_ranking") === lit(1))


    val outlierObjectRDD = result2.rdd.map { row =>
      val outlier = new BrasCoutOutlier(
        row.getAs[String]("bras_id"),
        row.getAs[Int]("signin_total_count"),
        row.getAs[Int]("logoff_total_count"),
        row.getAs[Double]("rateSL"),
        row.getAs[Double]("rateLS"),
        row.getAs[java.sql.Timestamp]("time")
      )
      outlier
    }

    import org.elasticsearch.spark._
    outlierObjectRDD.foreachPartition { partition =>
      if (partition.hasNext) {
        val arrayListType = new TypeToken[java.util.ArrayList[BrasCoutOutlier]]() {}.getType
        val gson = new Gson()
        val metrics = new util.ArrayList[BrasCoutOutlier]()
        partition.foreach(bras => metrics.add(bras))
        val metricsJson = gson.toJson(metrics, arrayListType)
        val http = Http(powerBIConfig.value("radius_anomaly_point_detect_url")).proxy(powerBIConfig.value("proxy_host"), 80)
        try {
          val result = http.postData(metricsJson)
            .header("Content-Type", "application/json")
            .header("Charset", "UTF-8")
            .option(HttpOptions.readTimeout(15000)).asString
          logger.info(s"Send Outlier metrics to PowerBi - Statuscode : ${result.statusLine}.")
        } catch {
          case e: java.net.SocketTimeoutException => logger.error(s"Time out Exception when sending Outlier result to BI")
          case _: Throwable => println("Just ignore this shit.")
        }
      }
    }
    Thread.sleep(70000)
    //ES -Mongo -Cassandra
    //outlierObjectRDD.saveToEs("radius_oulier_detect")
  }
}

class doJob(sparkSession: SparkSession, powerBIConfig: Broadcast[Map[String, String]]) extends TimerTask with Serializable {
  def run(): Unit = {
    read(sparkSession, powerBIConfig)
  }

  import scala.language.implicitConversions

  implicit def finiteDurationToSparkDuration(value: FiniteDuration): Duration = new Duration(value.toMillis)

  def logger = Logger.getLogger(getClass)

  def read(sparkSession: SparkSession, powerBIConfig: Broadcast[Map[String, String]]): Unit = {
    import sparkSession.implicits._
    val sc = sparkSession.sparkContext

    def spark = sparkSession.sqlContext

    val now = System.currentTimeMillis()
    val timestamp = new org.joda.time.DateTime(now).minusMinutes(16).toString("yyyy-MM-dd HH:mm:ss.SSS")

    val brasCounDF = spark.sql(s"SELECT * FROM brasscount WHERE time > '$timestamp'")
    println()
    println("COUNT :" + brasCounDF.count())
    println("TIME :" + timestamp)
    val rated = brasCounDF.withColumn("rateSL", ($"signin_total_count") / ($"logoff_total_count" + 1))
                          .withColumn("rateLS", ($"logoff_total_count") / ($"signin_total_count" + 1))
    // CALCULATE TREND AND DETREND
    val upperBound = new UpperIQR
    val winMed = new MovingMedian
    val window = Window.partitionBy("bras_id").orderBy($"time").rowsBetween(-4, 0)
    val window2 = Window.partitionBy("bras_id").orderBy($"time")
    val window3 = Window.partitionBy("bras_id").orderBy($"time".desc)
    //MOVING MEDIAN - TREND
    val movingMedian = rated.withColumn("moving_medianSL", winMed(col("rateSL")).over(window))
      .withColumn("moving_medianLS", winMed(col("rateLS")).over(window))
    //DETREND
    val detrend = movingMedian.withColumn("detrendSL", ($"rateSL" - $"moving_medianSL"))
      .withColumn("detrendLS", ($"rateLS" - $"moving_medianLS"))
    //TODO Consider to do moving median again.
    val iqr = detrend.withColumn("upper_iqr_SL", upperBound(col("detrendSL")).over(window2))
      .withColumn("upper_iqr_LS", upperBound(col("detrendLS")).over(window2))

    val result: DataFrame = iqr.withColumn("outlier", when(($"detrendSL" > $"upper_iqr_SL" && $"upper_iqr_SL" > lit(0))
      || ($"detrendLS" > $"upper_iqr_LS" && $"upper_iqr_LS" > lit(0)), 1).otherwise(0))
      .withColumn("time_ranking", rank().over(window3))
    result.show()
    /*val result = iqr.withColumn("outlier",when(($"detrendSL" > $"upper_iqr_SL" )
                           || ($"detrendLS" > $"upper_iqr_LS" ),1).otherwise(0))*/
    //result.show()
    import org.elasticsearch.spark.sql._
    //result.saveToES("")
    val result2: DataFrame = result.select("bras_id", "signin_total_count", "logoff_total_count", "rateSL", "rateLS", "time")
      //.where($"outlier" > lit(0))
        //TODO uncomment this on  production mode
      .where($"outlier" > lit(0) && col("time_ranking") === lit(1))
      result2.show(30)
/*    // DETECT DIRECTLY ON RATE
    val iqr = detrend.withColumn("upper_iqr_SL", upperBound(col("rateSL")).over(window))
      .withColumn("upper_iqr_LS", upperBound(col("detrendLS")).over(window))*/

    val outlierObjectRDD = result2.rdd.map { row =>
      val outlier = new BrasCoutOutlier(
        row.getAs[String]("bras_id"),
        row.getAs[Int]("signin_total_count"),
        row.getAs[Int]("logoff_total_count"),
        row.getAs[Double]("rateSL"),
        row.getAs[Double]("rateLS"),
        row.getAs[java.sql.Timestamp]("time")
      )
      outlier
    }

    import org.elasticsearch.spark._
    outlierObjectRDD.foreachPartition { partition =>
      if (partition.hasNext) {
        val arrayListType = new TypeToken[java.util.ArrayList[BrasCoutOutlier]]() {}.getType
        val gson = new Gson()
        val metrics = new util.ArrayList[BrasCoutOutlier]()
        partition.foreach(bras => metrics.add(bras))
        val metricsJson = gson.toJson(metrics, arrayListType)
        val http = Http(powerBIConfig.value("radius_anomaly_point_detect_url")).proxy(powerBIConfig.value("proxy_host"), 80)
        try {
          val result = http.postData(metricsJson)
            .header("Content-Type", "application/json")
            .header("Charset", "UTF-8")
            .option(HttpOptions.readTimeout(15000)).asString
          logger.info(s"Send Outlier metrics to PowerBi - Statuscode : ${result.statusLine}.")
        } catch {
          case e: java.net.SocketTimeoutException => logger.error(s"Time out Exception when sending Outlier result to BI")
          case _: Throwable => println("Just ignore this shit.")
        }
      }
    }
    Thread.sleep(70000)
    //ES -Mongo -Cassandra
    //outlierObjectRDD.saveToEs("radius_oulier_detect")
  }
}
object timeTest {
  def main(args: Array[String]): Unit = {
    val now = System.currentTimeMillis()
    val date: DateTime = new org.joda.time.DateTime(now).minusMillis(16)
    //.formatted("yyyy-MM-dd HH:mm:ss.SSS")
    println(date.toString("yyyy-MM-dd HH:mm:ss.SSS"))
  }
}
case class BrasCountObject(
                            bras_id: String,
                            signin_total_count: Int,
                            logoff_total_count: Int,
                            signin_distinct_count: Int,
                            logoff_distinct_count: Int,
                            time: java.sql.Timestamp) extends Serializable {
}
case class BrasCoutOutlier(bras_id: String,
                           signin_total_count: Int,
                           logoff_total_count: Int,
                           rateSL: Double,
                           rateLS: Double,
                           time: Timestamp
                          ) extends Serializable {}