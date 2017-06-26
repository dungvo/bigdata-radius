package streaming_jobs.test

import java.util.concurrent.TimeUnit

import core.sources.KafkaDStreamSource
import core.streaming.{SparkLogLevel, SparkStreamingApplication}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 25/06/2017.
  */
object EmptyInput extends SparkStreamingApplication{
  override def streamingBatchDuration: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)
           def windowDuration: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)
           def slideDuration: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)

  override def streamingCheckpointDir: String = "/tmp/"

  override def sparkConfig: Map[String, String] = Predef.Map( "spark.master"->"local[2]",
                                                              "spark.app.name"->"test-empty-input",
                                                              "spark.cassandra.connection.host" -> "localhost",
                                                              "spark.cassandra.output.batch.size.rows" -> "auto")
  def kafkaSource: Map[String,String] = Predef.Map("auto.offset.reset"-> "latest",
                                  "bootstrap.servers"-> "localhost:9092",
                                  //"bootstrap.servers"-> "172.27.11.75:9092,172.27.11.80:9092,172.27.11.85:9092",
                                  //"bootstrap.servers": "localhost:9092"
                                  "group.id"-> "raidus-streaming-log",
                                  //String De
                                  "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
                                  "value.deserializer"-> "org.apache.kafka.common.serialization.StringDeserializer")
  def kafkaInputTopic = "input"
  def run(ssc: StreamingContext,
          ss: SparkSession,
          lines: DStream[String]): Unit = {
    implicit def finiteDurationToSparkDuration(value: FiniteDuration): Duration = new Duration(value.toMillis)
    val sc = ssc.sparkContext

    lines.foreachRDD{
      line =>
        val spark = ss.sqlContext
        val now = System.currentTimeMillis()
        val timestamp = new org.joda.time.DateTime(now).minusMinutes(16).toString("yyyy-MM-dd HH:mm:ss.SSS")
        val brasCounDF = spark.sql(s"SELECT * FROM brasscount ")
        //val brasCounDF = spark.sql(s"SELECT * FROM brasscount WHERE time > '$timestamp'")
        println()
        println("COUNT :" + brasCounDF.count())
        println("TIME :" + timestamp)
    }
  }
  def start(source: KafkaDStreamSource,dll: String): Unit ={
    withSparkStreamingContext{(ss,ssc) =>
      val input: DStream[String] = source.createSource(ssc,kafkaInputTopic)
      val spark = ss.sqlContext
      spark.sql(dll)
      run(ssc,ss,input)

    }
  }

  def main(args: Array[String]): Unit = {
    SparkLogLevel.setStreamingLogLevels()
    val source = KafkaDStreamSource(kafkaSource)
    val createDDL: String =
      """CREATE TEMPORARY VIEW brasscount
     USING org.apache.spark.sql.cassandra
     OPTIONS (
     table "brasscount",
     keyspace "radius",
     pushdown "true")"""
    start(source,createDDL)
  }



}
