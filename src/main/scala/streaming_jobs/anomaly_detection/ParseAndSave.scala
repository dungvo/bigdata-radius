package streaming_jobs.anomaly_detection

import java.sql.Timestamp

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import  com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
/**
  * Created by hungdv on 14/06/2017.
  */
object ParseAndSave {
  def parseAndSave(ssc:StreamingContext,
                   ss: SparkSession,
                   lines: DStream[String],
                   cassandraConfig: Map[String,String]
                  ) :Unit ={
    val objectDStream: DStream[BrasCountObject] = lines.transform(reconstructObject)

    // SAVE TO CASSANDRA
    objectDStream.saveToCassandra(cassandraConfig("keySpace").toString,
                                   cassandraConfig("table").toString,
                                   SomeColumns("bras_id","signin_total_count","logoff_total_count","signin_distinct_count","logoff_distinct_count","time"))
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
