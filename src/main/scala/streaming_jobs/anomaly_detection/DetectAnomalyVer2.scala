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
import util.DatetimeController
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

        val brasCounDFRaw = spark.sql(s"SELECT * FROM brasscount WHERE time > '$timestamp'").cache()
        //println("COUNTRAW :" + brasCounDFRaw.count())
        val brasCounDFrank = brasCounDFRaw.withColumn("rank_time",rank().over(window3)).cache()
        val brasCounDF= brasCounDFrank.select("*").where(col("rank_time") <= lit(15)).cache()
        //val newestBras = brasCounDFrank.select("*").where(col("rank_time") === lit(1)).cache()
        brasCounDFrank.unpersist()
        //println()
        //println("COUNT :" + brasCounDF.count())
        //println("TIME :" + timestamp)

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
        //println("RESULT-------------------------------------------------------")
        // TODO debug
        //result.show(40)
        import org.elasticsearch.spark.sql._
        //result.saveToES("")
        val result2: DataFrame = result.select("bras_id", "signin_total_count", "logoff_total_count", "rateSL", "rateLS", "time","rank_time")
                                        .where($"outlier" > lit(0) && col("rank_time") === lit(1))
                                        .cache()
        //TODO debug
        //println("RESULT 2 :  --------------------------------------------------")
        //result2.show()
        val brasids = result2.select("bras_id").rdd.map(r => r(0)).collect()
        var brasIdsString = "("
        brasids.foreach{x =>
          val y = "'" + x + "',"
          brasIdsString = brasIdsString + y
        }
        brasIdsString = brasIdsString.dropRight(1) + ")"
        // TODO debug
        //println("Bras String :  ------------------------------------  ---------")
        //println(brasIdsString)
        val theshold = spark.sql(s"Select * from bras_theshold WHERE bras_id IN $brasIdsString").cache()
        // TODO debug
        //println("theshold :  ---------------------------------     ------------")
        //theshold.show()
       /* val result3 = result2.join(theshold,"bras_id").select("bras_id", "signin_total_count", "logoff_total_count", "rateSL", "rateLS", "time")
          .where(($"signin_total_count" >= $"threshold_signin" && $"signin_total_count" > lit(30)) || ($"logoff_total_count" >= $"threshold_logoff" && $"logoff_total_count" > lit(30)))
          .cache()*/
        val result3tmp = result2.join(theshold,"bras_id").select("bras_id", "signin_total_count", "logoff_total_count", "rateSL", "rateLS", "time").cache()
        val result3 = result3tmp.select("*")
         .where(($"signin_total_count" >= $"threshold_signin" && $"signin_total_count" > lit(30)) || ($"logoff_total_count" >= $"threshold_logoff" && $"logoff_total_count" > lit(30)))
         result3.cache()
        // TODO Debug
        println("result3 :----- ----")
        result3.show()
        //.where($"outlier" > lit(0) && col("time_ranking") === lit(1))
        println("RESULT FILTERD : Result 3 ------------------------------------")


        if(result3.count > 0){
          val bras_result3_ids = result3.select("bras_id").rdd.map(r => r(0)).collect()
          if(bras_result3_ids.length > 0){
            var bras_result3_IdsString = "("
            bras_result3_ids.foreach{x =>
              val y = "'" + x + "',"
              bras_result3_IdsString = bras_result3_IdsString + y
            }
            bras_result3_IdsString = bras_result3_IdsString.dropRight(1) + ")"
            println("bras_id : " + bras_result3_IdsString)

            val timestamp_mapping = new org.joda.time.DateTime(now).minusMinutes(2).toString("yyyy-MM-dd HH:mm:ss.SSS")
            val brashostMapping = spark.sql(s"Select * from brashostmapping WHERE bras_id IN $bras_result3_IdsString").cache()
            println("Bras host mapping")
            brashostMapping.show()

            val hostIds = brashostMapping.select("host").rdd.map(r => r(0)).collect()
            if(hostIds.length > 0){
              var host_IdsString = "("
              hostIds.foreach{x =>
                val y = "'" + x + "',"
                host_IdsString = host_IdsString + y
              }
              host_IdsString = host_IdsString.dropRight(1) + ")"

              println("host id : " + host_IdsString)
              val noc_bras_error = spark.sql(s"Select * from noc_bras_error_counting WHERE devide IN $bras_result3_IdsString AND time > '$timestamp_mapping'").cache()
              println("noc_bras_error")
              noc_bras_error.show()
              val inf_host_error = spark.sql(s"Select * from inf_host_error_counting WHERE host IN $host_IdsString AND time > '$timestamp_mapping'").cache()
              println("inf_host_error")
              inf_host_error.show()
              val noc_be_sum = noc_bras_error.groupBy(col("devide")).agg(sum("total_info_count").as("info_status"),sum("total_critical_count").as(("critical_status")))
                .withColumnRenamed("devide","bras_id")

              val inf_he_sum = inf_host_error.groupBy(col("host")).agg(sum("cpe_error").as("cpe_error_status_inf"), sum("lostip_error").as("lostip_error_status_inf"))
              val inf_mapping = inf_he_sum.join(brashostMapping,"host")
              val inf_sum_by_bras = inf_mapping.groupBy(col("bras_id")).agg(sum("cpe_error_status_inf").as("cpe_error_status"), sum("lostip_error_status_inf").as("lostip_error_status"))

              val result_noc_inf = result3.join(inf_sum_by_bras,"bras_id").join(noc_be_sum,"bras_id")
                .select("bras_id", "signin_total_count", "logoff_total_count", "rateSL", "rateLS", "time","cpe_error_status","lostip_error_status","info_status","critical_status")
                .cache()
              println("FINAL RESULS -----------")
              result_noc_inf.show
              result_noc_inf.unpersist()
              noc_bras_error.unpersist()
              inf_host_error.unpersist()
            }


            brashostMapping.unpersist()
          }

        }

        // MAPPING HERE.


        /*if(result_noc_inf.count() > 0){
          try{
            val outlierObjectRDD = result_noc_inf.rdd.map { row =>
              try{
                val time = row.getAs[java.sql.Timestamp]("time")
                val outlier = new BrasCoutOutlier(
                  row.getAs[String]("bras_id"),
                  row.getAs[Int]("signin_total_count"),
                  row.getAs[Int]("logoff_total_count"),
                  row.getAs[Double]("rateSL"),
                  row.getAs[Double]("rateLS"),
                  time,
                  DatetimeController.sqlTimeStampToNumberFormat(time),
                  row.getAs[Int]("cpe_error_status"),
                  row.getAs[Int]("lostip_error_status"),
                  row.getAs[Int]("info_status"),
                  row.getAs[Int]("critical_status")
                )
                println("OUTLIER : -----------------------------------------------------")
                println(outlier)
                outlier
              }catch {
                case e: Exception => {println("ERROR IN PARSING BLOCK + "+e.printStackTrace()) ;
                  BrasCoutOutlier("n/a",0,0,0,0,new Timestamp(0),0,0,0,0,0)}
                //case _: Throwable => println("Throwable ")
              }

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
            case e : Throwable => println("ERROR IN SENDING BLOCK !!---------------------------------------------------" + e.printStackTrace())
          }

        }*/
        // Unpersist
        brasCounDFRaw.unpersist()
        brasCounDF.unpersist()
        result2.unpersist()
        result3.unpersist()
        theshold.unpersist()
        result3tmp.unpersist()

        val now2 = System.currentTimeMillis()
        val timeExecute = (now2 - now)/1000
        println("Execution time for batch : " + timeExecute + " s ")
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
                           time: Timestamp,
                           timeInNumber: Float,
                          cpe_error_status: Int,
                          lostip_error_status: Int,
                          info_status: Int,
                          critical_status: Int
                          ) extends Serializable{}