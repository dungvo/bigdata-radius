package streaming_jobs.anomaly_detection

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.elasticsearch.spark._
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming._
import core.udafs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import streaming_jobs.conn_jobs.BrasCountObject

import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex
import scalaj.http.{Http, HttpOptions}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import streaming_jobs.conn_jobs.ParseAndCountConnLog.getClass
import org.elasticsearch.spark.sql._

/**
  * Created by hungdv on 12/06/2017.
  */
object DetectAnomaly {
  val logger = Logger.getLogger(getClass)
  def detect(ssc: StreamingContext,
             ss: SparkSession,
             lines: DStream[String],
             windowDuration: FiniteDuration,
             slideDuration: FiniteDuration,
             topics: String,
             powerBIConfig: Predef.Map[String,String]
            ): Unit ={
    import scala.language.implicitConversions
    implicit def finiteDurationToSparkDuration(value: FiniteDuration): Duration = new Duration(value.toMillis)
    import ss.implicits._
    val sc = ssc.sparkContext
    val bPowerBIURL: Broadcast[String] = sc.broadcast(powerBIConfig("radius_anomaly_point_detect_url"))
    //val objectBrasCount: DStream[BrasCountObject] = lines.transform(reconstructObject)
    //lines.foreachRDD{_ => {}}
//    ssc.start()
    try{
      while( ssc.getState == StreamingContextState.ACTIVE){
        val time = new Time(System.currentTimeMillis)
        val intervalBegin = time.floor(Seconds(windowDuration.toSeconds))
        val seqRDD: Seq[RDD[String]] = lines.slice(intervalBegin,intervalBegin + Seconds(windowDuration.toSeconds))
        val rdds = seqRDD.reduceLeft(_ union _)
            val rdd = rdds.map{
              line =>
                val splited = line.split("#")
                val brasCountObject = new BrasCountObject(splited(0),splited(1).toInt,splited(2).toInt,splited(3).toInt,splited(4).toInt,stringToSqlTimestamp(splited(5)))
                brasCountObject
            }
            rdd.saveToEs("radius_outlier/test")
           val context = rdd.sparkContext
           val brasCounDF = rdd.toDF("bras_id","signin_total_count","logoff_total_count","signin_distinct_count","logoff_distinct_count","time")
           val rated = brasCounDF.withColumn("rateSL",($"signin_total_count")/($"logoff_total_count"))
                                 .withColumn("rateLS",($"logoff_total_count")/($"signin_total_count"))

           val upperBound = new UpperIQR
           val winMed = new MovingMedian
           val window = Window.partitionBy("bras_id").orderBy($"time").rowsBetween(-4,0)
           val window2 = Window.partitionBy("bras_id").orderBy($"time")
           val window3 = Window.partitionBy("bras_id").orderBy($"time".desc)
           val movingMedian = rated.withColumn("moving_medianSL",winMed(col("rateSL")).over(window))
                                     .withColumn("moving_medianLS",winMed(col("rateLS")).over(window))
           val detrend = movingMedian.withColumn("detrendSL",($"rateSL" - $"moving_medianSL"))
                                     .withColumn("detrendLS",($"rateLS" - $"moving_medianLS"))
           //TODO Consider to do moving median again.
           val iqr = detrend.withColumn("upper_iqr_SL",upperBound(col("detrendSL")).over(window2))
             .withColumn("upper_iqr_LS",upperBound(col("detrendLS")).over(window2))

           val result: DataFrame = iqr.withColumn("outlier",when(($"detrendSL" > $"upper_iqr_SL" && $"upper_iqr_SL" > lit(0))
                                                   || ($"detrendLS" > $"upper_iqr_LS" && $"upper_iqr_LS" > lit(0)),1).otherwise(0))
                                        .withColumn("time_ranking",rank().over(window3))
            /*val result = iqr.withColumn("outlier",when(($"detrendSL" > $"upper_iqr_SL" )
                                   || ($"detrendLS" > $"upper_iqr_LS" ),1).otherwise(0))*/


          import org.elasticsearch.spark.sql._
          //result.saveToES("")
           val result2: DataFrame = result.select("bras_id","signin_total_count","logoff_total_count","rateSL","rateLS","time")
                                          .where($"outlier" > lit(0) && col("time_ranking") === lit(1))

           val outlierObjectRDD = result2.rdd.map{row =>
             val outlier = new BrasCoutOutlier(
               row.getAs[String]("bras_id"),
               row.getAs[Int]("signin_total_count"),
               row.getAs[Int]("logoff_total_count"),
               row.getAs[java.sql.Timestamp]("time"),
               0,0,0,0,0,0,0,0,0,0
             )
             outlier
           }

           import org.elasticsearch.spark._
           /*if(outlierObjectRDD.count() > 0) {
             outlierObjectRDD.saveToEs("outlier/outlierObjectRDD")
             //val count = outlierObjectRDD.count()
             //context.makeRDD(Seq(Map("count" -> count))).saveToEs("outlierCount/count")
           }*/
           outlierObjectRDD.foreachPartition{partition =>
             if(partition.hasNext){
               val arrayListType = new TypeToken[java.util.ArrayList[BrasCoutOutlier]]() {}.getType
               val gson = new Gson()
               val metrics = new util.ArrayList[BrasCoutOutlier]()
               partition.foreach(bras => metrics.add(bras))
               val metricsJson = gson.toJson(metrics, arrayListType)
               val http = Http(bPowerBIURL.value).proxy(powerBIConfig("proxy_host"),80)
               try{
                 val result = http.postData(metricsJson)
                   .header("Content-Type", "application/json")
                   .header("Charset", "UTF-8")
                   .option(HttpOptions.readTimeout(15000)).asString
                 logger.info(s"Send Outlier metrics to PowerBi - Statuscode : ${result.statusLine}.")
               }catch
                 {
                   case e:java.net.SocketTimeoutException => logger.error(s"Time out Exception when sending Outlier result to BI")
                   case _: Throwable => println("Just ignore this shit.")
                 }
             }
           }

      }
    }

    //lines.compute()
    //lines.slice()s
    //val seqRDD: Seq[RDD[BrasCountObject]] = objectBrasCount.slice(intervalBegin,intervalBegin + slideDuration)
    //val seqRDD: Seq[RDD[String]] = lines.slice(intervalBegin,intervalBegin + slideDuration)
    //val rdd: RDD[String] = seqRDD.reduceLeft(_ union _)
    //import org.elasticsearch.spark._
    //if(rdd.count() > 0) {
    //  rdd.saveToEs("unionRDD/string")
      //val count = outlierObjectRDD.count()
      //context.makeRDD(Seq(Map("count" -> count))).saveToEs("outlierCount/count")
    //}
    //val rdd: RDD[BrasCountObject] = seqRDD.reduceLeft(_ union _)




    /*objectBrasCount.foreachRDD{
      rdd =>
        val context = rdd.sparkContext
        val brasCounDF = rdd.toDF("bras_id","signin_total_count","logoff_total_count","signin_distinct_count","logoff_distinct_count","time")
        val rated = brasCounDF.withColumn("rateSL",($"signin_total_count")/($"logoff_total_count"))
                              .withColumn("rateLS",($"logoff_total_count")/($"signin_total_count"))



        val upperBound = new UpperIQR
        val winMed = new MovingMedian
        val window = Window.partitionBy("bras_id").orderBy($"time").rowsBetween(-4,0)
        val window2 = Window.partitionBy("bras_id").orderBy($"time")
        val movingMedian = rated.withColumn("moving_medianSL",winMed(col("rateSL")).over(window))
                                  .withColumn("moving_medianLS",winMed(col("rateLS")).over(window))
        val detrend = movingMedian.withColumn("detrendSL",($"rateSL" - $"moving_medianSL"))
                                  .withColumn("detrendLS",($"rateLS" - $"moving_medianLS"))
        //TODO Consider to do moving median again.
        val iqr = detrend.withColumn("upper_iqr_SL",upperBound(col("detrendSL")).over(window2))
          .withColumn("upper_iqr_LS",upperBound(col("detrendLS")).over(window2))
        val result = iqr.withColumn("outlier",when(($"detrendSL" > $"upper_iqr_SL" && $"upper_iqr_SL" > lit(0))
                                                || ($"detrendLS" > $"upper_iqr_LS" && $"upper_iqr_LS" > lit(0)),1).otherwise(0))
         /*val result = iqr.withColumn("outlier",when(($"detrendSL" > $"upper_iqr_SL" )
                                || ($"detrendLS" > $"upper_iqr_LS" ),1).otherwise(0))*/
        val result2: DataFrame = result.select("bras_id","signin_total_count","logoff_total_count","rateSL","rateLS","time")
          .where($"outlier" > lit(0))

        val outlierObjectRDD = result2.rdd.map{row =>
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
        /*if(outlierObjectRDD.count() > 0) {
          outlierObjectRDD.saveToEs("outlier/outlierObjectRDD")
          //val count = outlierObjectRDD.count()
          //context.makeRDD(Seq(Map("count" -> count))).saveToEs("outlierCount/count")
        }*/
        outlierObjectRDD.foreachPartition{partition =>
          if(partition.hasNext){
            val arrayListType = new TypeToken[java.util.ArrayList[BrasCoutOutlier]]() {}.getType
            val gson = new Gson()
            val metrics = new util.ArrayList[BrasCoutOutlier]()
            partition.foreach(bras => metrics.add(bras))
            val metricsJson = gson.toJson(metrics, arrayListType)
            val http = Http(bPowerBIURL.value).proxy(powerBIConfig("proxy_host"),80)
            try{
              val result = http.postData(metricsJson)
                .header("Content-Type", "application/json")
                .header("Charset", "UTF-8")
                .option(HttpOptions.readTimeout(15000)).asString
              logger.info(s"Send Outlier metrics to PowerBi - Statuscode : ${result.statusLine}.")
            }catch
              {
                case e:java.net.SocketTimeoutException => logger.error(s"Time out Exception when sending Outlier result to BI")
                case _: Throwable => println("Just ignore this shit.")
              }
          }
        }

    }*/

  }
  def reconstructObject = (lines: RDD[String]) => lines.map{
    line =>

      val splited = line.split("#")
      val brasCountObject = new BrasCountObject(splited(0),splited(1).toInt,splited(2).toInt,splited(3).toInt,splited(4).toInt,stringToSqlTimestamp(splited(5)))
      brasCountObject
  }
  def stringWithTimeZoneToSqlTimestamp(string: String): Timestamp ={
    val date: DateTime = DateTime.parse(string,DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ") )
    val timstamp = new Timestamp(date.getMillis)
    timstamp
  }
  def stringToSqlTimestamp(string: String): Timestamp ={
    val date: DateTime = DateTime.parse(string,DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS") )
    val timstamp = new Timestamp(date.getMillis)
    timstamp
  }
}
object test{
  def main(args: Array[String]): Unit = {
    val string = "2017-06-10T04:00:13.800+07:00"
    val date = DateTime.parse(string,DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ") )
    val timestamp = new Timestamp(date.getMillis)
    println(timestamp)
    val string2  = "2017-06-12 19:38:32.431"
    "2017-06-12 19:54:17.887"
    val date2 = DateTime.parse(string2,DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS") )
    val timestamp2 = new Timestamp(date2.getMillis)
    //val time = stringWithTimeZoneToSqlTimestamp(string)
    println(timestamp2)
    val string3 = "QNH-MP02#7#5#7#5#2017-06-13 07:37:45.774"
    val arr = string3.split("#")
    val list = arr.toList
    println(list)
    arr.foreach(println(_))
    println(arr)
    val string4 = "Opview-NOCHN34#3#0#1#0#2017-06-13 08:27:15.375"
    val arr2 = string4.split("#")
    arr2.foreach(println(_))
  }
  def stringWithTimeZoneToSqlTimestamp(string: String): Timestamp ={
    val formater = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
    //val formater = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:SSSz")
    //val formater = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS Z")
    //val formater = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    //val formater = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz")
    val date = formater.parse(string)

    val timeStamp = new Timestamp(date.getTime())
    timeStamp
  }
}
