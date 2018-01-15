package streaming_jobs.inf_jobs

import java.sql.{SQLException, Timestamp}
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import util.DatetimeController
import org.apache.spark.sql.functions._
import storage.postgres.PostgresIO

import scala.collection.mutable.ListBuffer

/**
  * Created by hungdv on 22/08/2017.
  */
object  PatternModuleDetect {
  def parseAndSave(ssc:StreamingContext,
                   ss: SparkSession,
                   lines: DStream[String],
                   postgresConfig: Map[String,String]) : Unit ={
    val objectDStream = lines.transform(reconstructObject)
   /* val moduleError = "module/cpe error"
    val portdownError = "user port down"
    val disconnectError = "disconnect/lost IP"*/
   val jdbcUrl = PostgresIO.getJDBCUrl(postgresConfig)
    //println("START INF JOB")
    val sc = ss.sparkContext
    val bJdbcURL = sc.broadcast(jdbcUrl)
    val pgProperties    = new Properties()
    pgProperties.setProperty("driver","org.postgresql.Driver")
    val bPgProperties   = sc.broadcast(pgProperties)
    import ss.implicits._
    //TEST
    //objectDStream.window(Duration(60*1000*1),Duration(5*1000)).foreachRDD{  rdd =>
    // PROD
   objectDStream.window(Duration(60*1000*3),Duration(30*1000)).foreachRDD{  (rdd, time: org.apache.spark.streaming.Time) =>
      val compositeKey = rdd.map{
            // Module,errorName,time
        tuple =>
          if(tuple._1 != "user port down")   {
            ((tuple._3.substring(0,tuple._3.lastIndexOf('/')),tuple._1),
              DatetimeController.stringToLong(tuple._2,"yyyy-MM-dd HH:mm:ss"))
          }else{
            ((tuple._3,tuple._1),
              DatetimeController.stringToLong(tuple._2,"yyyy-MM-dd HH:mm:ss"))
          }
      }

      val groupByModuleAndError = compositeKey.groupByKey()

      val candidates: RDD[(String, Iterable[(String, List[Long])])] = groupByModuleAndError.map{
        pair =>
          val moduleAndError = pair._1 // (module,error)
          val listTimeValues = pair._2
          (moduleAndError._1,(moduleAndError._2/*,listTimeValues.size*/,listTimeValues.toList))
      }.groupByKey()

     //candidates.foreach(println(_))

      val result: RDD[(String, Long)] = candidates.map{
        candidate =>
          if(candidate._2.size >= 3){
            val map: Map[String, List[Long]] = candidate._2.toMap
            val moduleErrorTimeList = map.getOrElse("module/cpe error",List())
            val userPortDownTimeList = map.getOrElse("user port down",List())

            val disconnectTimeList: List[Long] = map.getOrElse("disconnect/lost IP",List())
            val disBuffer = disconnectTimeList.to[ListBuffer]

            val powerOffTimeList = map.getOrElse("power off",List())

            powerOffTimeList.foreach{x =>
              disconnectTimeList.foreach{y =>
                if(Math.abs(x - y) < 30000) disBuffer -= y
              }
            }

            val filterdDisconnect = disBuffer.toList

   /*         if(userPortDownTimeList.size > 0){
              println()
              println("---------------------------------------------")
              println("user port down " + userPortDownTimeList.size)
              println("disconnect Time List " + disconnectTimeList.size)
              println("filterd Disconnect " + filterdDisconnect.size)
              println("module Error TimeList " + moduleErrorTimeList.size)
              println()
              println("---------------------------------------------")

            }
            println("-------" + time)*/


            if(moduleErrorTimeList.size >= 10
              && filterdDisconnect.size >=1
              //&& disconnectTimeList.size >= 1
              && userPortDownTimeList.size >=1
              && filterdDisconnect.min < moduleErrorTimeList.max
              && filterdDisconnect.min < userPortDownTimeList.max  ){
              (candidate._1,filterdDisconnect)
            }else{
              (null,List(0L))
            }
          }
          else{
            (null,List(0L))
          }

      }.filter{ x => x._1 != null}.flatMap{case(key,values) => values.map((key, _))}

      val resultDF = result.toDF("module","time")

      val compositeKeyIndex = rdd.map{
        // Module,errorName,time
        tuple => (tuple._3,tuple._3.substring(0,tuple._3.length -3),tuple._1,
          DatetimeController.stringToLong(tuple._2,"yyyy-MM-dd HH:mm:ss"))
      }

      val rddRawDf = compositeKeyIndex.toDF("index","module","erro","time")

      val outlier = rddRawDf.join(resultDF,Seq("module","time"),"right_outer")
            .drop("erro")
            .drop("module")

      //println("OUTLIER : ")
      //outlier.show()

      val outlierCounting = outlier
          .groupBy("index").agg(count("time").as("erro_count_by_index"))
        //.groupBy("module").agg(countDistinct(col("index")).as("count_err_by_index"))
        .withColumn("server_time",org.apache.spark.sql.functions.current_timestamp())
     //DEBUG
     /*if(outlierCounting.count() > 0){
       outlierCounting.show()
     }*/

      //Save To Postgres.
     try{
       PostgresIO.writeToPostgres(ss, outlierCounting, bJdbcURL.value, "disconect_pattern", SaveMode.Append, bPgProperties.value)
     }catch{
       case e: SQLException => System.err.println("SQLException occur when save disconect_pattern : " + e.getSQLState + " " + e.getMessage)
       case e: Exception => System.err.println("UncatchException occur when save disconect_pattern : " +  e.getMessage)
       case _ => println("Ignore !")
     }
    }
  }
  def reconstructObject = (lines: RDD[String]) => lines.map{
    line =>
      val splited: Array[String] = line.split("#")
      if(splited.length >=3){
        val tuple = (splited(0),splited(1),splited(2))
        tuple
      }else ("n/a","n/a","n/a")

  }

}
