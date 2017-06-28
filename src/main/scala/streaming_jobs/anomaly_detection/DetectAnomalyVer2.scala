package streaming_jobs.anomaly_detection

import java.sql.Timestamp

import core.udafs.{MovingMedian, UpperIQR}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import streaming_jobs.anomaly_detection.DetectAnomaly.getClass
import org.apache.spark.sql.functions._

import scala.concurrent.duration.FiniteDuration
import scalaj.http.{Http, HttpOptions, HttpResponse}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.joda.time.DateTime

import scala.concurrent.duration.FiniteDuration
import java.util
/**
  * Created by hungdv on 25/06/2017.
  */
object DetectAnomalyVer2 {
  val logger = Logger.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  def detect(ssc: StreamingContext,
             ss: SparkSession,
             lines: DStream[String],
             topics: String,
             powerBIConfig: Predef.Map[String,String]
            ): Unit = {
    implicit def finiteDurationToSparkDuration(value: FiniteDuration): Duration = new Duration(value.toMillis)

    val sc = ssc.sparkContext
    val bPowerBIURL = sc.broadcast(powerBIConfig("radius_anomaly_point_detect_url"))
    val bPowerBIProxyHost = sc.broadcast(powerBIConfig("proxy_host"))
    lines.foreachRDD {
      line =>
        import ss.implicits._
        val sc = ss.sparkContext

        def spark = ss.sqlContext
        val upperBound = new UpperIQR
        val winMed = new MovingMedian
        //val window = Window.partitionBy("bras_id").orderBy($"time").rowsBetween(-4, 0)
        // Donot use orderBy here.
        val window2 = Window.partitionBy("bras_id")
        //val window2 = Window.partitionBy("bras_id").orderBy($"time")
        val window3 = Window.partitionBy("bras_id").orderBy($"time".desc)
        val now = System.currentTimeMillis()
        val timestamp = new org.joda.time.DateTime(now).minusMinutes(30).toString("yyyy-MM-dd HH:mm:ss.SSS")

        val brasCounDFRaw = spark.sql(s"SELECT * FROM brasscount WHERE time > '$timestamp'")
        println("COUNTRAW :" + brasCounDFRaw.count())
        val brasCounDFrank = brasCounDFRaw.withColumn("rank_time",rank().over(window3))
        val brasCounDF= brasCounDFrank.select("*").where(col("rank_time") <= lit(15))
        println()
        println("COUNT :" + brasCounDF.count())
        println("TIME :" + timestamp)

        val rated = brasCounDF.withColumn("rateSL", ($"signin_total_count") / ($"logoff_total_count" + 1))
          .withColumn("rateLS", ($"logoff_total_count") / ($"signin_total_count" + 1))


       /* val movingMedian = rated.withColumn("moving_medianSL", winMed(col("rateSL")).over(window))
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
*/
       val iqr = rated.withColumn("upper_iqr_SL", upperBound(col("rateSL")).over(window2))
         .withColumn("upper_iqr_LS", upperBound(col("rateLS")).over(window2))

        val result: DataFrame = iqr.withColumn("outlier", when(($"rateSL" > $"upper_iqr_SL" && $"upper_iqr_SL" > lit(0))
          || ($"rateLS" > $"upper_iqr_LS" && $"upper_iqr_LS" > lit(0)), 1).otherwise(0))
          //.withColumn("time_ranking", rank().over(window3))
          //.select("*")
          //.where($"bras_id" === "BLC-MP01-2" || $"bras_id" === "BLC-MP01-2")
        /*val result = iqr.withColumn("outlier",when(($"detrendSL" > $"upper_iqr_SL" )
                               || ($"detrendLS" > $"upper_iqr_LS" ),1).otherwise(0))*/
        println("RESULT-------------------------------------------------------")
        //result.show(40)
        import org.elasticsearch.spark.sql._
        //result.saveToES("")
        val result2: DataFrame = result.select("bras_id", "signin_total_count", "logoff_total_count", "rateSL", "rateLS", "time")
          //.where($"outlier" > lit(0))
          //TODO uncomment this on  production mode
          .where($"outlier" > lit(0) && col("rank_time") === lit(1))
        //.where($"outlier" > lit(0) && col("time_ranking") === lit(1))
        println("RESULT FILTERD-------------------------------------------------------")
        result2.show()
        try{
          val outlierObjectRDD = result2.rdd.map { row =>
            val outlier = new BrasCoutOutlier(
              row.getAs[String]("bras_id"),
              row.getAs[Int]("signin_total_count"),
              row.getAs[Int]("logoff_total_count"),
              row.getAs[Double]("rateSL"),
              row.getAs[Double]("rateLS"),
              row.getAs[java.sql.Timestamp]("time")
            )
            println("OUTLIER : ---------------------------------------------------------")
            println(outlier)
            outlier
          }
          println("SEND  TO BI -----------------------------------------------------")
          import org.elasticsearch.spark._
          outlierObjectRDD.foreachPartition { partition =>
            if (partition.hasNext) {
              val arrayListType = new TypeToken[java.util.ArrayList[BrasCoutOutlier]]() {}.getType
              val gson = new Gson()
              val metrics = new util.ArrayList[BrasCoutOutlier]()
              partition.foreach(bras => metrics.add(bras))
              val metricsJson = gson.toJson(metrics, arrayListType)
              println("METRICS : " + metricsJson )
              val http = Http(bPowerBIURL.value).proxy(bPowerBIProxyHost.value, 80)
              try {
                val result = http.postData(metricsJson)
                  .header("Content-Type", "application/json")
                  .header("Charset", "UTF-8")
                  .option(HttpOptions.readTimeout(15000)).asString
                println(s"Send Outlier metrics to PowerBi - Statuscode : ${result.statusLine}.")
                logger.warn(s"Send Outlier metrics to PowerBi - Statuscode : ${result.statusLine}.")
              } catch {
                case e: java.net.SocketTimeoutException => logger.error(s"Time out Exception when sending Outlier result to BI")
                case _: Throwable => println("Just ignore this shit.")
              }
            }
          }
        }catch{
          case e : Throwable => println("ERROR IN SENDING BLOCK !!---------------------------------------------------")
        }

      //ES -Mongo -Cassandra
      //outlierObjectRDD.saveToEs("radius_oulier_detect")
    }
  }

}
case class BrasCountObject(
                            bras_id: String,
                            signin_total_count :Int,
                            logoff_total_count :Int,
                            signin_distinct_count :Int,
                            logoff_distinct_count :Int,
                            time :java.sql.Timestamp) extends Serializable{
}
case class BrasCoutOutlier(bras_id: String,
                           signin_total_count :Int,
                           logoff_total_count :Int,
                           rateSL: Double,
                           rateLS: Double,
                           time: Timestamp
                          ) extends Serializable{}