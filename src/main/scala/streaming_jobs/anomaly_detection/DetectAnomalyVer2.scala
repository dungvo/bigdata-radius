package streaming_jobs.anomaly_detection

import java.io.Serializable
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException, Timestamp}
import java.text.SimpleDateFormat

import core.udafs.{MovingMedian, UpperIQR}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import streaming_jobs.anomaly_detection.DetectAnomaly.getClass
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

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
import java.util.{Calendar, Date, Properties}

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.sql._
import com.redis.RedisClient
import core.streaming.RedisClientFactory
import org.apache.spark.rdd.RDD
import storage.postgres.PostgresIO

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
             powerBIConfig: Predef.Map[String, String],
             postgresConfig: Map[String, String]
            ): Unit = {
    implicit def finiteDurationToSparkDuration(value: FiniteDuration): Duration = new Duration(value.toMillis)

    val sc = ssc.sparkContext
    val bPowerBIURL = sc.broadcast(powerBIConfig("radius_anomaly_point_detect_url"))
    val bPowerBIProxyHost = sc.broadcast(powerBIConfig("proxy_host"))

    val jdbcUrl = PostgresIO.getJDBCUrl(postgresConfig)
    println(jdbcUrl)
    val bJdbcURL = sc.broadcast(jdbcUrl)
    val pgProperties = new Properties()
    pgProperties.setProperty("driver", "org.postgresql.Driver")
    val bPgProperties = sc.broadcast(pgProperties)

    lines.foreachRDD {
      line =>
        val now = System.currentTimeMillis()
        val context = line.sparkContext
        import ss.implicits._
        val sc = ss.sparkContext

        def spark = ss.sqlContext

        val upperBound = new UpperIQR
        val winMed = new MovingMedian
        val normal: (String => String) = (arg: String) => {
          "normal"
        }
        val sqlAlwaysNormal = org.apache.spark.sql.functions.udf(normal)

        val outlier: (String => String) = (arg: String) => {
          "outlier"
        }
        val sqlAlwaysOutlier = org.apache.spark.sql.functions.udf(outlier)

        //val window = Window.partitionBy("bras_id").orderBy($"time").rowsBetween(-4, 0)
        // Donot use orderBy here.
        val window2 = Window.partitionBy("bras_id")
        //val window2 = Window.partitionBy("bras_id").orderBy($"time")
        val window3 = Window.partitionBy("bras_id").orderBy($"time".desc)

        //Get last 30 mins of bras_count
        val timestamp_last30mins = new org.joda.time.DateTime(now).minusMinutes(30).toString("yyyy-MM-dd HH:mm:ss.SSS")
        //Get last 2 mins of other sources.
        val timestamp_last2mins  = new org.joda.time.DateTime(now).minusMinutes(2).toString("yyyy-MM-dd HH:mm:ss.SSS")
        val timestamp_last15mins = new org.joda.time.DateTime(now).minusMinutes(15).toString("yyyy-MM-dd HH:mm:ss.SSS")
        // Load bras-detail.
        //val brasDetail  = PostgresIO.pushDownJDBCQuery("","")
        //brasDetail.cache()
        // Delect from bras-detail -> bras count.

        // TODO Remove this.
        //val brasCounDFRaw = spark.sql(s"SELECT * FROM brasscount WHERE time > '$timestamp'").cache()
        // Read bras detail from last 30 mins
        // Query phai dat dang alias, khong co dau ; at the end of query.

        val getBrasDetailQuery = s" (SELECT bras.bras_id,bras.time, bras.signin_total_count,bras.logoff_total_count," +
          s" bras.signin_distinct_count, bras.logoff_distinct_count,kibana.crit_kibana , kibana.info_kibana , opsview.unknown_opsview , opsview.warn_opsview , " +
          s" opsview.ok_opsview , opsview.crit_opsview , inf.cpe_error , inf.lostip_error  " +
          s" FROM " +
          s" (SELECT * FROM bras_count WHERE bras_count.time > '$timestamp_last30mins') AS bras left join " +
          s" (SELECT bras_id,SUM(total_critical_count) as crit_kibana,SUM(total_info_count) as info_kibana " +
          s" FROM dwh_kibana_agg WHERE dwh_kibana_agg.date_time > '$timestamp_last2mins '  " +
          s" GROUP BY bras_id) AS kibana on bras.bras_id = kibana.bras_id left join   " +
          s" (SELECT bras_id,SUM(unknown_opsview) as unknown_opsview, SUM(warn_opsview) as warn_opsview, SUM(ok_opsview) as ok_opsview,SUM(crit_opsview) as crit_opsview " +
          s" FROM dwh_opsview_status  WHERE dwh_opsview_status.date_time > '$timestamp_last15mins ' GROUP BY bras_id) AS opsview  on bras.bras_id = opsview.bras_id  left join  " +
          s" (SELECT bras_id,SUM(cpe_error) as cpe_error, SUM(lostip_error) as lostip_error " +
          s" FROM dwh_inf_host  WHERE dwh_inf_host.date_time > '$timestamp_last2mins ' GROUP BY bras_id) AS inf  on bras.bras_id = inf.bras_id ) as temp_table  "



        //println(getBrasDetailQuery)
        //Push down to db. server side join.
        val brasCounDFRaw = PostgresIO.pushDownQuery(ss, bJdbcURL.value, getBrasDetailQuery, bPgProperties.value)
                // Rank by time
        val brasCounDFrank = brasCounDFRaw.withColumn("rank_time", rank().over(window3)).cache()
        // Select signin-logoff -> detect outlier
        val brasCounDF = brasCounDFrank.select("bras_id", "signin_total_count", "logoff_total_count","time","rank_time",
          "signin_distinct_count","logoff_distinct_count").where(col("rank_time") <= lit(15)).cache()
        // Select  newest data to merge and update (avoid duplicate by select newest data).
        val newestBras = brasCounDFrank.where(col("rank_time") === lit(1)).drop(col("rank_time")).cache()
        brasCounDFrank.unpersist()
        val rated = brasCounDF.na.fill(0)
          .withColumn("rateSL", ($"signin_total_count") / ($"logoff_total_count" + 1))
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
        import org.elasticsearch.spark.sql._
        //result.saveToES("")
        val result2: DataFrame = result
          .select("bras_id", "signin_total_count", "logoff_total_count", "rateSL", "rateLS", "time", "rank_time","signin_distinct_count","logoff_distinct_count")
          .where($"outlier" > lit(0) && col("rank_time") === lit(1))
          .cache()
        val brasids = result2.select("bras_id").rdd.map(r => r(0)).collect()
        var brasIdsString = "("
        brasids.foreach { x =>
          val y = "'" + x + "',"
          brasIdsString = brasIdsString + y
        }
        brasIdsString = brasIdsString.dropRight(1) + " )"
        // TODO migrate to SQL
        // Get thres hold db for specific bras_ids
        // Cassandra version
        //val theshold = spark.sql(s"Select * from bras_theshold WHERE bras_id IN $brasIdsString").cache()
        // Postgres version:
        val thesholdQuery = s"( Select * from threshold WHERE bras_id IN $brasIdsString ) as bhm "
        //println("thresholdQuery " + thesholdQuery)

        val theshold = PostgresIO.pushDownQuery(ss, bJdbcURL.value,thesholdQuery,
          bPgProperties.value).cache()

        /* val result3 = result2.join(theshold,"bras_id").select("bras_id", "signin_total_count", "logoff_total_count", "rateSL", "rateLS", "time")
           .where(($"signin_total_count" >= $"threshold_signin" && $"signin_total_count" > lit(30)) || ($"logoff_total_count" >= $"threshold_logoff" && $"logoff_total_count" > lit(30)))
           .cache()*/
        val active_user = PostgresIO.loadTable(ss,bJdbcURL.value,"active_user",bPgProperties.value)
            .withColumnRenamed("active_users","active_user")
            .cache()


        val result3tmp = result2.join(theshold, Seq("bras_id"), "left_outer").na.fill(0)
          .select("bras_id", "signin_total_count", "logoff_total_count", "rateSL", "rateLS", "time","signin_distinct_count","logoff_distinct_count")
          .where(($"signin_total_count" >= $"threshold_signin" && $"signin_total_count" > lit(30)) || ($"logoff_total_count" >= $"threshold_logoff" && $"logoff_total_count" > lit(30)))
        //// Remove after test:
        //result3tmp.cache()
        if(result3tmp.count() > 0){
          val bras_result3_ids_df = result3tmp.select("bras_id").cache()

          ///
          // Get brasresult3_ids and save to postgres
          // create table :
          //
          ///


          val outlier_with_status = bras_result3_ids_df.withColumn("label", sqlAlwaysOutlier(col("bras_id")))
          bras_result3_ids_df.unpersist()
          val savedToDB_DF = newestBras.join(outlier_with_status, Seq("bras_id"), "left_outer").na.fill("normal").withColumnRenamed("time","date_time")
          println("SAVE TO DB detail 2")
          //savedToDB_DF.show(10)
          try {
            // TODO change to upsert.
            //PostgresIO.writeToPostgres(ss, savedToDB_DF, bJdbcURL.value, "dwh_radius_bras_detail", SaveMode.Append, bPgProperties.value)
            //Upsert to postgres
            upsertDetail2ToPostgres(ss,savedToDB_DF,bJdbcURL.value)
            //Upsert

          } catch {
            case e: SQLException => println("Error when saving data to pg dt 2" + e.getMessage + e.getMessage + e.getStackTrace)
            case e: Exception => println("Uncatched - Error when saving data to pg dt 2 " + e.getMessage + e.getStackTrace)
            case _ => println("Dont care :))")
          }

          val sendToBI_DF = savedToDB_DF.where("label = 'outlier'").drop("label").drop("signin_distinct_count").drop("logoff_distinct_count")

          try {
            val outlierObjectRDD = sendToBI_DF.rdd.map { row =>
              try {
                val time = row.getAs[java.sql.Timestamp]("date_time")
                val outlier = new BrasCoutOutlier(
                  row.getAs[String]("bras_id"),
                  row.getAs[Int]("signin_total_count"),
                  row.getAs[Int]("logoff_total_count"),
                  time,
                  DatetimeController.sqlTimeStampToNumberFormat(time),
                  row.getAs[Long]("cpe_error"),
                  row.getAs[Long]("lostip_error"),
                  row.getAs[Long]("crit_kibana"),
                  row.getAs[Long]("info_kibana"),
                  row.getAs[Long]("crit_opsview"),
                  row.getAs[Long]("ok_opsview"),
                  row.getAs[Long]("warn_opsview"),
                  row.getAs[Long]("unknown_opsview"),
                  //TODO - update active user.
                  0
                )
                println("OUTLIER without active user : -----------------------------------------------------")
                println(outlier)
                outlier
              } catch {
                case e: Exception => {
                  println("ERROR IN PARSING BLOCK dt2 + " + e.printStackTrace() + " " + e.getMessage +  " " + row.toString());
                  BrasCoutOutlier("n/a", 0, 0, new Timestamp(0), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                }
                //case _: Throwable => println("Throwable ")
              }

            }.filter(x => x.bras_id != "n/a")
            println("SEND  TO BI 2 -----------------------------------------------------")
            import org.elasticsearch.spark._
            outlierObjectRDD.foreachPartition { partition =>
              if (partition.hasNext) {
                val arrayListType = new TypeToken[java.util.ArrayList[BrasCoutOutlier]]() {}.getType
                val gson = new Gson()
                val metrics = new util.ArrayList[BrasCoutOutlier]()
                partition.foreach(bras => metrics.add(bras))
                val metricsJson = gson.toJson(metrics, arrayListType)
                println("METRICS 2 : " + metricsJson)
                val anomaly_bi2 = "https://api.powerbi.com/beta/4ebc9261-871a-44c5-93a5-60eb590917cd/datasets/bbfce1fb-fc9a-4a20-bee7-d3979235c70b/rows?key=AeUtdQT5bhT6AQOtZX886DdUWXGGTN56RuPpvFcslPtRFE2740JV%2Fd2Va3ed67HI5ba0bWR0MlklVNFoD2bBcw%3D%3D"
                val http = Http(anomaly_bi2).proxy(bPowerBIProxyHost.value, 80)
                try {
                  val result = http.postData(metricsJson)
                    .header("Content-Type", "application/json")
                    .header("Charset", "UTF-8")
                    .option(HttpOptions.readTimeout(15000)).asString
                  println(s"Send Outlier metrics to PowerBi - chart 2 - Statuscode : ${result.statusLine}.")
                  logger.warn(s"Send Outlier metrics to PowerBi - chart 2 - Statuscode : ${result.statusLine}.")
                } catch {
                  case e: java.net.SocketTimeoutException => logger.error(s"Time out Exception when sending Outlier result to BI")
                  case _: Throwable => println("Just ignore this shit.")
                }
              }
            }
          } catch {
            case e: Throwable => println("ERROR IN SENDING BLOCK !!---------------------------------------------------" + e.printStackTrace())
          }
        }

        val result3 = result3tmp.join(active_user,Seq("bras_id"), "left_outer").where($"signin_total_count" / $"active_user" > lit(0.01) || $"logoff_total_count" / $"active_user" > lit(0.01))
        result3.cache()
        // TODO Debug
        //println("result3 :----- ----" + result3.count())
        //result3.show()
        //.where($"outlier" > lit(0) && col("time_ranking") === lit(1))
        println("RESULT FILTERD : Result 3 ------------------------------------")

        // TODO : Doan code nay dung de luu toan bo trang thai cua 160 con brases [metrics,label]
        // metrics: sign,log off, rate, bras errors host errors
        // label: normal or outlier.
        // TODO Uncomment this to save all bras with status result to mongo
        // Change date : 18/07/2017
        // Ban dau su dung de cho front end - nodejs + mongodb
        // Nhung vi front-end lam xau qua nen bo.
        // Sau nay kha nang se khong dung mongo.
        // TODO : Do not remove this comment block!
        /*  if (result3.count > 0) {
          val bras_result3_ids_df = result3.select("bras_id").cache()
          val bras_result3_ids = bras_result3_ids_df.rdd.map(r => r(0)).collect()
          val outlier_with_status = bras_result3_ids_df.withColumn("label", sqlAlwaysOutlier(col("bras_id")))
          bras_result3_ids_df.unpersist()
          val mongoDF = newestBras.join(outlier_with_status, Seq("bras_id"), "left_outer").na.fill("normal")
          // Save to Mongo.
          try {
            //MongoSpark.save(mongoDF.write.option("collection","mongodb://172.27.11.146:27017/radius.outlier").mode("append"))
            //println("Mongo DF ")
            //mongoDF.show
            mongoDF.write.mode("append").mongo(WriteConfig(Map("collection" -> "outlier"), Some(WriteConfig(context))))
            //mongoDF.write.mode("append").mongo()
          } catch {
            case e: Exception => println("Error when saving data to Mongo " + e.getMessage)
            case _ => println("Dont care :))")
          }


          if (bras_result3_ids.length > 0) {
            var bras_result3_IdsString = "("
            bras_result3_ids.foreach { x =>
              val y = "'" + x + "',"
              bras_result3_IdsString = bras_result3_IdsString + y
            }
            bras_result3_IdsString = bras_result3_IdsString.dropRight(1) + ")"
            println("bras_id : " + bras_result3_IdsString)

            val timestamp_mapping = new org.joda.time.DateTime(now).minusMinutes(2).toString("yyyy-MM-dd HH:mm:ss.SSS")
            // time in Noc is in GMT +0
            //val noc_timestamp_mapping = new org.joda.time.DateTime(now).minusMinutes(2).toString("yyyy-MM-dd HH:mm:ss.SSS") + "Z"
            //val noc_timestamp_mapping2 = new org.joda.time.DateTime(now).minusHours(7).minusMinutes(2).toString("yyyy-MM-dd HH:mm:ss.SSS") + "Z"
            val brashostMapping = spark.sql(s"Select * from brashostmapping WHERE bras_id IN $bras_result3_IdsString").cache()
            //println("Bras host mapping")
            //brashostMapping.show()

            val hostIds: Array[Any] = brashostMapping.select("host").rdd.map(r => r(0)).collect()
            if (hostIds.length > 0) {
              var host_IdsString = "("
              hostIds.foreach { x =>
                val y = "'" + x + "',"
                host_IdsString = host_IdsString + y
              }
              host_IdsString = host_IdsString.dropRight(1) + ")"

              println("host id : " + host_IdsString)
              val noc_bras_error = spark.sql(s"Select * from noc_bras_error_counting WHERE devide IN $bras_result3_IdsString AND time > '$timestamp_mapping'").cache()
              println("noc_bras_error")
              //println("Noc Query0 : " + s"Select * from noc_bras_error_counting WHERE devide IN $bras_result3_IdsString AND time > '$timestamp_mapping'")
              //println("Noc Query1 : " + s"Select * from noc_bras_error_counting WHERE devide IN $bras_result3_IdsString AND time > '$noc_timestamp_mapping'")
              //println("Noc Query2 : " + s"Select * from noc_bras_error_counting WHERE devide IN $bras_result3_IdsString AND time > '$noc_timestamp_mapping2'")
              noc_bras_error.show()
              val inf_host_error = spark.sql(s"Select * from inf_host_error_counting WHERE host IN $host_IdsString AND time > '$timestamp_mapping'").cache()
              //println("INF Query0 : " + s"Select * from inf_host_error_counting WHERE host IN $host_IdsString AND time > '$timestamp_mapping'")
              //println("inf_host_error")
              //inf_host_error.show()
              val noc_be_sum = noc_bras_error.groupBy(col("devide")).agg(sum("total_info_count").as("info_status"), sum("total_critical_count").as(("critical_status")))
                .withColumnRenamed("devide", "bras_id")
              //println("noc_be_sum ")
              //noc_be_sum.show()
              val inf_he_sum = inf_host_error.groupBy(col("host")).agg(sum("cpe_error").as("cpe_error_status_inf"), sum("lostip_error").as("lostip_error_status_inf"))
              val inf_mapping = inf_he_sum.join(brashostMapping, "host")
              val inf_sum_by_bras = inf_mapping.groupBy(col("bras_id")).agg(sum("cpe_error_status_inf").as("cpe_error_status"), sum("lostip_error_status_inf").as("lostip_error_status"))
              val result_inf = result3.join(inf_sum_by_bras, Seq("bras_id"), "left_outer")
              val result_noc_inf = result_inf.join(noc_be_sum, Seq("bras_id"), "left_outer")
                .select("bras_id", "signin_total_count", "logoff_total_count", "rateSL", "rateLS", "time", "cpe_error_status", "lostip_error_status", "info_status", "critical_status").na.fill(-1)
                .cache()
              println("FINAL RESULS -----------")
              result_noc_inf.show
              //println(result_noc_inf.schema)
              if (result_noc_inf.count() > 0) {
                try {
                  val outlierObjectRDD = result_noc_inf.rdd.map { row =>
                    try {
                      val time = row.getAs[java.sql.Timestamp]("time")
                      val outlier = new BrasCoutOutlier(
                        row.getAs[String]("bras_id"),
                        row.getAs[Int]("signin_total_count"),
                        row.getAs[Int]("logoff_total_count"),
                        row.getAs[Double]("rateSL"),
                        row.getAs[Double]("rateLS"),
                        time,
                        DatetimeController.sqlTimeStampToNumberFormat(time),
                        row.getAs[Long]("cpe_error_status"),
                        row.getAs[Long]("lostip_error_status"),
                        row.getAs[Long]("info_status"),
                        row.getAs[Long]("critical_status")
                      )
                      println("OUTLIER : -----------------------------------------------------")
                      println(outlier)
                      outlier
                    } catch {
                      case e: Exception => {
                        println("ERROR IN PARSING BLOCK + " + e.printStackTrace());
                        BrasCoutOutlier("n/a", 0, 0, 0, 0, new Timestamp(0), 0, 0, 0, 0, 0)
                      }
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
                      println("METRICS : " + metricsJson)
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
                } catch {
                  case e: Throwable => println("ERROR IN SENDING BLOCK !!---------------------------------------------------" + e.printStackTrace())
                }

              }
              result_noc_inf.unpersist()
              noc_bras_error.unpersist()
              inf_host_error.unpersist()

            }

            brashostMapping.unpersist()
          }
        } else {
          val mongoDF = newestBras.withColumn("label", sqlAlwaysNormal(col("bras_id")))
          try {
            println("Mongo DF ")
            //mongoDF.show
            mongoDF.write.mode("append").mongo(WriteConfig(Map("collection" -> "outlier"), Some(WriteConfig(context))))
            //mongoDF.write.mode("append").mongo()
            //MongoSpark.save(mongoDF.write.option("collection","mongodb://172.27.11.146:27017/radius.outlier").mode("append"))
          } catch {
            case e: Exception => println("Error when saving data to Mongo " + e.getStackTrace)
            case _ => println("Dont care :))")
          }
          //save to mongo.
        }*/

        // TODO VERSION 2 : Thay vi gui toi mongo full list, se tam bo di.
        // Start date 17-07-2017
        // Change date 01-08-2017
        /*if (result3.count > 0) {
          val bras_result3_ids_df = result3.select("bras_id").cache()
          val bras_result3_ids = bras_result3_ids_df.rdd.map(r => r(0)).collect()
          val outlier_with_status = bras_result3_ids_df.withColumn("label", sqlAlwaysOutlier(col("bras_id")))
          bras_result3_ids_df.unpersist()

          if (bras_result3_ids.length > 0) {
            var bras_result3_IdsString = "("
            bras_result3_ids.foreach { x =>
              val y = "'" + x + "',"
              bras_result3_IdsString = bras_result3_IdsString + y
            }
            bras_result3_IdsString = bras_result3_IdsString.dropRight(1) + ")"
            println("bras_id : " + bras_result3_IdsString)
            val timestamp_mapping = new org.joda.time.DateTime(now).minusMinutes(2).toString("yyyy-MM-dd HH:mm:ss.SSS")
            val brashostMapping = spark.sql(s"Select * from brashostmapping WHERE bras_id IN $bras_result3_IdsString").cache()
            val hostIds: Array[Any] = brashostMapping.select("host").rdd.map(r => r(0)).collect()
            if (hostIds.length > 0) {
              var host_IdsString = "("
              hostIds.foreach { x =>
                val y = "'" + x + "',"
                host_IdsString = host_IdsString + y
              }
              host_IdsString = host_IdsString.dropRight(1) + ")"

              println("host id : " + host_IdsString)
              val noc_bras_error = spark.sql(s"Select * from noc_bras_error_counting WHERE devide IN $bras_result3_IdsString AND time > '$timestamp_mapping'").cache()
              println("noc_bras_error")
              noc_bras_error.show()
              val inf_host_error = spark.sql(s"Select * from inf_host_error_counting WHERE host IN $host_IdsString AND time > '$timestamp_mapping'").cache()
              val noc_be_sum = noc_bras_error.groupBy(col("devide")).agg(sum("total_info_count").as("info_status"), sum("total_critical_count").as(("critical_status")))
                .withColumnRenamed("devide", "bras_id")
              val inf_he_sum = inf_host_error.groupBy(col("host")).agg(sum("cpe_error").as("cpe_error_status_inf"), sum("lostip_error").as("lostip_error_status_inf"))
              val inf_mapping = inf_he_sum.join(brashostMapping, "host")
              val inf_sum_by_bras = inf_mapping.groupBy(col("bras_id")).agg(sum("cpe_error_status_inf").as("cpe_error_status"), sum("lostip_error_status_inf").as("lostip_error_status"))
              val result_inf = result3.join(inf_sum_by_bras, Seq("bras_id"), "left_outer")
              val result_noc_inf = result_inf.join(noc_be_sum, Seq("bras_id"), "left_outer")
                .select("bras_id", "signin_total_count", "logoff_total_count", "rateSL", "rateLS", "time", "cpe_error_status", "lostip_error_status", "info_status", "critical_status").na.fill(-1)
                .cache()
              println("FINAL RESULS -----------")
              result_noc_inf.show
              if (result_noc_inf.count() > 0) {
                try {
                  val outlierObjectRDD = result_noc_inf.rdd.map { row =>
                    try {
                      val time = row.getAs[java.sql.Timestamp]("time")
                      val outlier = new BrasCoutOutlier(
                        row.getAs[String]("bras_id"),
                        row.getAs[Int]("signin_total_count"),
                        row.getAs[Int]("logoff_total_count"),
                        row.getAs[Double]("rateSL"),
                        row.getAs[Double]("rateLS"),
                        time,
                        DatetimeController.sqlTimeStampToNumberFormat(time),
                        row.getAs[Long]("cpe_error_status"),
                        row.getAs[Long]("lostip_error_status"),
                        row.getAs[Long]("info_status"),
                        row.getAs[Long]("critical_status")
                      )
                      println("OUTLIER : -----------------------------------------------------")
                      println(outlier)
                      outlier
                    } catch {
                      case e: Exception => {
                        println("ERROR IN PARSING BLOCK + " + e.printStackTrace());
                        BrasCoutOutlier("n/a", 0, 0, 0, 0, new Timestamp(0), 0, 0, 0, 0, 0)
                      }
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
                      println("METRICS : " + metricsJson)
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
                } catch {
                  case e: Throwable => println("ERROR IN SENDING BLOCK !!---------------------------------------------------" + e.printStackTrace())
                }

              }
              result_noc_inf.unpersist()
              noc_bras_error.unpersist()
              inf_host_error.unpersist()

            }

            brashostMapping.unpersist()
          }
        }     */
        //TODO VERSION 3:
        // Start date 01-08-2017
        if (result3.count > 0) {
          ///////////// User LogOff
          //
          val bras_result3_ids_outlier_df = result3.select("bras_id")
          val bras_result3_ids_outlier: RDD[String] = bras_result3_ids_outlier_df.rdd.map(r => r(0).toString)
          //val bras_result3_ids: Array[Any] = bras_result3_ids_outlier_df.rdd.map(r => r(0)).collect()
          //val redis = RedisClientFactory.getOrCreateClient("172.27.11.141",6373)

          bras_result3_ids_outlier.foreachPartition { part =>
            //val r = new RedisClient("172.27.11.141", 6373)
            try {
              val clients = RedisClientFactory.getOrCreateClient(("172.27.11.141", 6373))
              println("Get new redis client in port " + clients.port)
              clients.withClient { client =>
                val iter = part.map { bras =>
                  //Current time
                  val key = getRedisKey(bras, 0)
                  // Current time minus 1,2,3 minute
                  val keyMinusOne = getRedisKey(bras, -1)
                  val keyMinusTwo = getRedisKey(bras, -2)
                  val keyMinusThree = getRedisKey(bras, -3)
                  val time = getCurrentTime()
                  //val list = r.lrange(key,0,-1)
                  val list = client.lrange(key, 0, -1)
                  val list1 = client.lrange(keyMinusOne, 0, -1)
                  val list2 = client.lrange(keyMinusTwo, 0, -1)
                  val list3 = client.lrange(keyMinusThree, 0, -1)

                  val finalList: List[Option[String]] = list.getOrElse(List(None)) ::: list1.getOrElse(List(None)) ::: list2.getOrElse(List(None)) ::: list3.getOrElse(List(None))

                  (key, bras, time, finalList.flatten.mkString(","))
                }
                println("Get values from redis susccessfully " )
                /* val rdd = sc.parallelize(iter.toSeq)
               val logoutUserDF = rdd.toDF("key","bras_id","time","users_list")
               logoutUserDF.show*/
                //val sql = s"INSERT INTO logoff_users(key,bras_id,time,user_list) values()"
                try {
                  println("Create new Postgres connection : ")
                  val conn: Connection = DriverManager.getConnection(bJdbcURL.value)

                  val preparedStatement = conn.prepareStatement(" INSERT INTO logoff_users(event_key,bras_id,time,user_list) values(?, ?, ?, ?) ;")

                  iter.foreach {
                    tuple =>

                      //DEBUG:
                      //println(tuple)
                      //
                      preparedStatement.setString(1, tuple._1)
                      preparedStatement.setString(2, tuple._2)
                      preparedStatement.setTimestamp(3, tuple._3)
                      preparedStatement.setString(4, tuple._4)
                      preparedStatement.addBatch()
                  }
                  preparedStatement.executeBatch()
                  conn.close()
                  println("Execute batch okey. ")
                  logger.info(s"Save batch of logoffuser successfully to postgres")
                } catch {
                  case e: SQLException => println("Error when saving data to pg logoff_users " + e.getMessage + e.getMessage + e.getStackTrace)
                  case e: Exception => println("Uncatched - Error when saving data to pg logoff_users  " + e.getMessage + e.getStackTrace)
                  case _ => println("Dont care :))")
                }

              }
              //
            }catch {
              case e: Exception => println("Uncatched - Error insight block Redis client.   " + e.getMessage + e.getStackTrace)
              case _ => println("Dont care :))")
            }

          }


          val bras_result3_ids_df = result3.select("bras_id","active_user").cache()
          val bras_result3_ids = bras_result3_ids_df.rdd.map(r => r(0)).collect()
          val outlier_with_status = bras_result3_ids_df.withColumn("label", sqlAlwaysOutlier(col("bras_id")))
          bras_result3_ids_df.unpersist()
          val savedToDB_DF = newestBras.join(outlier_with_status, Seq("bras_id"), "left_outer").na.fill("normal").withColumnRenamed("time","date_time")
          //println("SAVE TO DB")
          //savedToDB_DF.show(10)
          try {
            // TODO change to upsert.
            //PostgresIO.writeToPostgres(ss, savedToDB_DF, bJdbcURL.value, "dwh_radius_bras_detail", SaveMode.Append, bPgProperties.value)
            //Upsert to postgres
            upsertDetailToPostgres(ss,savedToDB_DF,bJdbcURL.value)
            logger.info("Upsert Bras Detail to DB successfully.")
            //Upsert

          } catch {
            case e: SQLException => println("Error when saving data to pg" + e.getMessage + e.getMessage + e.getStackTrace)
            case e: Exception => println("Uncatched - Error when saving data to pg " + e.getMessage + e.getStackTrace)
            case _ => println("Dont care :))")
          }

          val sendToBI_DF = savedToDB_DF.where("label = 'outlier'").drop("label").drop("signin_distinct_count").drop("logoff_distinct_count")

          try {
            val outlierObjectRDD = sendToBI_DF.rdd.map { row =>
              try {
                val time = row.getAs[java.sql.Timestamp]("date_time")
                val outlier = new BrasCoutOutlier(
                  row.getAs[String]("bras_id"),
                  row.getAs[Int]("signin_total_count"),
                  row.getAs[Int]("logoff_total_count"),
                  time,
                  DatetimeController.sqlTimeStampToNumberFormat(time),
                  row.getAs[Long]("cpe_error"),
                  row.getAs[Long]("lostip_error"),
                  row.getAs[Long]("crit_kibana"),
                  row.getAs[Long]("info_kibana"),
                  row.getAs[Long]("crit_opsview"),
                  row.getAs[Long]("ok_opsview"),
                  row.getAs[Long]("warn_opsview"),
                  row.getAs[Long]("unknown_opsview"),
                  //TODO - update active user.
                  row.getAs[Int]("active_user")
                )
                println("OUTLIER : -----------------------------------------------------")
                println(outlier)
                outlier
              } catch {
                case e: Exception => {
                  println("ERROR IN PARSING BLOCK + " + e.printStackTrace() + " " + e.getMessage +  " " + row.toString())
                  BrasCoutOutlier("n/a", 0, 0, new Timestamp(0), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                }
                //case _: Throwable => println("Throwable ")
              }

            }.filter(x => x.bras_id != "n/a")
            println("SEND  TO BI -----------------------------------------------------")
            import org.elasticsearch.spark._
            outlierObjectRDD.foreachPartition { partition =>
              if (partition.hasNext) {
                val arrayListType = new TypeToken[java.util.ArrayList[BrasCoutOutlier]]() {}.getType
                val gson = new Gson()
                val metrics = new util.ArrayList[BrasCoutOutlier]()
                partition.foreach(bras => metrics.add(bras))
                val metricsJson = gson.toJson(metrics, arrayListType)
                println("METRICS : " + metricsJson)
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
          } catch {
            case e: Throwable => println("ERROR IN SENDING BLOCK !!---------------------------------------------------" + e.printStackTrace())
          }

        }else{
          val savedToDB_DF = newestBras.join(active_user,Seq("bras_id"), "left_outer")
            .withColumn("label", sqlAlwaysNormal(col("bras_id"))).withColumnRenamed("time","date_time")
          //println("SAVE TO DB")
          //savedToDB_DF.show(10)
          try {
            //Upsert to postgres :
            upsertDetailToPostgres(ss,savedToDB_DF,bJdbcURL.value)
            // Dupplicate orccur
            //PostgresIO.writeToPostgres(ss, savedToDB_DF, bJdbcURL.value, "dwh_radius_bras_detail", SaveMode.Append, bPgProperties.value)
          } catch {
            case e: SQLException => println("Error when saving data to pg - [else block]" + e.getMessage  + e.getStackTrace )
            case e: Exception => println("Uncatched - Error when saving data to pg - [else block] " + e.getMessage  + e.getStackTrace )
            case _ => println("Dont care :))")
          }
        }


        // Unpersist
        brasCounDFRaw.unpersist()
        brasCounDF.unpersist()
        result2.unpersist()
        result3.unpersist()
        theshold.unpersist()
        active_user.unpersist()
        result3tmp.unpersist()
        newestBras.unpersist()
        val now2 = System.currentTimeMillis()
        val timeExecute = (now2 - now) / 1000
        println("Execution time for batch : " + timeExecute + " s ")
      //ES -Mongo -Cassandra
      //outlierObjectRDD.saveToEs("radius_oulier_detect")
    }
  }

  /**
    * Upsert bras detail table to Postgres
    * Handle with duplicate.
    * So much hard code here.
    * @param ss
    * @param savedToDB_DF
    * @param jdbcURL
    */
  def upsertDetailToPostgres(ss: SparkSession,savedToDB_DF: DataFrame,jdbcURL: String) = {
    import ss.implicits._
    savedToDB_DF
      //.na.fill(0)
      .repartition(6)
      .foreachPartition{batch =>
      val conn: Connection = DriverManager.getConnection(jdbcURL)
      val st: PreparedStatement = conn.prepareStatement("INSERT INTO dwh_radius_bras_detail(bras_id,date_time,active_user," +
        "signin_total_count,logoff_total_count,signin_distinct_count,logoff_distinct_count,crit_kibana,info_kibana," +
        "unknown_opsview,warn_opsview,ok_opsview,crit_opsview,cpe_error,lostip_error,label) " +
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
        " ON CONFLICT (bras_id,date_time) DO UPDATE  " +
        " SET signin_total_count = excluded.signin_total_count, " +
        " active_user = excluded.active_user, " +
        " logoff_total_count = excluded.logoff_total_count, " +
        " signin_distinct_count = excluded.signin_distinct_count, " +
        " logoff_distinct_count = excluded.logoff_distinct_count, " +
        " cpe_error = excluded.cpe_error, " +
        " lostip_error = excluded.lostip_error, " +
        " crit_kibana = excluded.crit_kibana, " +
        " info_kibana = excluded.info_kibana, " +
        " crit_opsview = excluded.crit_opsview, " +
        " ok_opsview = excluded.ok_opsview, " +
        " warn_opsview = excluded.warn_opsview, " +
        " unknown_opsview = excluded.unknown_opsview, " +
        " label = excluded.label ;"
      )
      batch.grouped(100).foreach { session =>

        session.foreach{ x =>
          st.setString(1,x.getString(0))
          st.setTimestamp(2,x.getTimestamp(1))
          st.setInt(3,x.getAs[Int]("active_user"))
          st.setInt(4,x.getAs[Int]("signin_total_count"))
   /*       st.setInt(4,x.getInt(3))
          st.setInt(5,x.getInt(4))
          st.setInt(6,x.getInt(5))
          st.setInt(7,x.getInt(6))
          st.setInt(8,x.getInt(7))
          st.setInt(9,x.getInt(8))
          st.setInt(10,x.getInt(9))
          st.setInt(11,x.getInt(10))
          st.setInt(12,x.getInt(11))
          st.setInt(13,x.getInt(12))
          st.setInt(14,x.getInt(13))
          st.setInt(15,x.getInt(14))
          st.setString(16,x.getString(15))*/
          st.setInt(5,x.getAs[Int]("logoff_total_count"))
          st.setInt(6,x.getAs[Int]("signin_distinct_count"))
          st.setInt(7,x.getAs[Int]("logoff_distinct_count"))
          st.setInt(8,x.getAs[Long]("cpe_error").toInt)
          st.setInt(9,x.getAs[Long]("lostip_error").toInt)
          st.setInt(10,x.getAs[Long]("crit_kibana").toInt)
          st.setInt(11,x.getAs[Long]("info_kibana").toInt)
          st.setInt(12,x.getAs[Long]("crit_opsview").toInt)
          st.setInt(13,x.getAs[Long]("ok_opsview").toInt)
          st.setInt(14,x.getAs[Long]("warn_opsview").toInt)
          st.setInt(15,x.getAs[Long]("unknown_opsview").toInt)
          st.setString(16,x.getAs[String]("label"))

          st.addBatch()
        }
        st.executeBatch()
      }
      conn.close()
      logger.info(s"Save batch ${batch.toString()} successfully")
    }
  }
  def upsertDetail2ToPostgres(ss: SparkSession,savedToDB_DF: DataFrame,jdbcURL: String) = {
    import ss.implicits._
    savedToDB_DF
      //.na.fill(0)
      .repartition(6)
      .foreachPartition{batch =>
        val conn: Connection = DriverManager.getConnection(jdbcURL)
        val st: PreparedStatement = conn.prepareStatement("INSERT INTO dwh_radius_bras_detail2(bras_id,date_time,active_user," +
          "signin_total_count,logoff_total_count,signin_distinct_count,logoff_distinct_count,crit_kibana,info_kibana," +
          "unknown_opsview,warn_opsview,ok_opsview,crit_opsview,cpe_error,lostip_error,label) " +
          " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)" +
          " ON CONFLICT (bras_id,date_time) DO UPDATE  " +
          " SET signin_total_count = excluded.signin_total_count, " +
          " active_user = excluded.active_user, " +
          " logoff_total_count = excluded.logoff_total_count, " +
          " signin_distinct_count = excluded.signin_distinct_count, " +
          " logoff_distinct_count = excluded.logoff_distinct_count, " +
          " cpe_error = excluded.cpe_error, " +
          " lostip_error = excluded.lostip_error, " +
          " crit_kibana = excluded.crit_kibana, " +
          " info_kibana = excluded.info_kibana, " +
          " crit_opsview = excluded.crit_opsview, " +
          " ok_opsview = excluded.ok_opsview, " +
          " warn_opsview = excluded.warn_opsview, " +
          " unknown_opsview = excluded.unknown_opsview, " +
          " label = excluded.label ;"
        )
        batch.grouped(100).foreach { session =>

          session.foreach{ x =>
            st.setString(1,x.getString(0))
            st.setTimestamp(2,x.getTimestamp(1))
            st.setInt(3,0)
            st.setInt(4,x.getAs[Int]("signin_total_count"))
            /*       st.setInt(4,x.getInt(3))
                   st.setInt(5,x.getInt(4))
                   st.setInt(6,x.getInt(5))
                   st.setInt(7,x.getInt(6))
                   st.setInt(8,x.getInt(7))
                   st.setInt(9,x.getInt(8))
                   st.setInt(10,x.getInt(9))
                   st.setInt(11,x.getInt(10))
                   st.setInt(12,x.getInt(11))
                   st.setInt(13,x.getInt(12))
                   st.setInt(14,x.getInt(13))
                   st.setInt(15,x.getInt(14))
                   st.setString(16,x.getString(15))*/
            st.setInt(5,x.getAs[Int]("logoff_total_count"))
            st.setInt(6,x.getAs[Int]("signin_distinct_count"))
            st.setInt(7,x.getAs[Int]("logoff_distinct_count"))
            st.setInt(8,x.getAs[Long]("cpe_error").toInt)
            st.setInt(9,x.getAs[Long]("lostip_error").toInt)
            st.setInt(10,x.getAs[Long]("crit_kibana").toInt)
            st.setInt(11,x.getAs[Long]("info_kibana").toInt)
            st.setInt(12,x.getAs[Long]("crit_opsview").toInt)
            st.setInt(13,x.getAs[Long]("ok_opsview").toInt)
            st.setInt(14,x.getAs[Long]("warn_opsview").toInt)
            st.setInt(15,x.getAs[Long]("unknown_opsview").toInt)
            st.setString(16,x.getAs[String]("label"))

            st.addBatch()
          }
          st.executeBatch()
        }
        conn.close()
        logger.info(s"Save batch ${batch.toString()} successfully")
      }
  }
  def getRedisKey(brasName: String,timeMinus: Int): String = {
    val key = brasName + "-" + getCurrentStringTime(timeMinus)
    key
  }
  def getCurrentStringTime(minus: Int):String ={
    val now = Calendar.getInstance().getTimeInMillis
    val target: Date = new Date(now + minus*60000)
    val nowFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val nowFormeted: String = nowFormater.format(target)
    //val nowFormeted = nowFormater.format(now).toString
    nowFormeted
  }
  def getCurrentStringTime():String ={
    val now: Date = Calendar.getInstance().getTime
    val nowFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    val nowFormeted: String = nowFormater.format(now)
    //val nowFormeted = nowFormater.format(now).toString
    nowFormeted
  }

  def getCurrentTime() :java.sql.Timestamp ={
    val timestamp = new Timestamp(System.currentTimeMillis())
    timestamp
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
                           time: Timestamp,
                           timeInNumber: Float,
                           cpe_error: Long,
                           lostip_error: Long,
                           crit_kibana: Long,
                           info_kibana: Long,
                           crit_opsview: Long,
                           ok_opsview: Long,
                           warn_opsview: Long,
                           unknown_opsview: Long,
                           active_user: Long
                          ) extends Serializable {}


object testTime {
  def main(args: Array[String]): Unit = {
    val now = System.currentTimeMillis()
    val time = new org.joda.time.DateTime(now).minusHours(7).minusMinutes(2).toString("yyyy-MM-dd HH:mm:ss.SSSZ")
    println(time)


    val minusOne = DetectAnomalyVer2.getCurrentStringTime(-2)
    println(minusOne)


    val a = List(Some("a"))
    val b = List(Some("b"))
    val c = List(Some("c"),Some("c"))
    val d = a:::b:::c
    println(d.flatten)
  }
}