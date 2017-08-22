package streaming_jobs.inf_jobs

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import util.DatetimeController

/**
  * Created by hungdv on 22/08/2017.
  */
object PatternModuleDetect {
  def parseAndSave(ssc:StreamingContext,
                   ss: SparkSession,
                   lines: DStream[String],
                   postgresConfig: Map[String,String]) : Unit ={
    val objectDStream = lines.transform(reconstructObject)
    val moduleError = ""
    val portdownError = ""
    val disconnectError = ""

    objectDStream.foreachRDD{  rdd =>
      val compositeKey = rdd.map{
            // Module,errorName,time
        tuple => ((tuple._3.substring(0,tuple._3.length -3),tuple._1),
                  DatetimeController.stringToLong(tuple._2,"yyyy-MM-dd HH:mm:ss"))
      }

      val groupByModuleAndError = compositeKey.groupByKey()

      val candidates: RDD[(String, Iterable[(String, Int, List[Long])])] = groupByModuleAndError.map{
        pair =>
          val moduleAndError = pair._1 // (module,error)
          val listTimeValues = pair._2
          (moduleAndError._1,(moduleAndError._2,listTimeValues.size,listTimeValues.toList.sorted))
      }.groupByKey()

      val result = candidates.map{
        candidate =>
          if(candidate._2.size == 3){
            val map = candidate._2.toMap
            if(map.get(""))
          }

      }






    }

  }
  def reconstructObject = (lines: RDD[String]) => lines.map{
    line =>

      val splited: Array[String] = line.split("#")
      val tuple = (splited(0),splited(1),splited(2))
      tuple
  }




}
