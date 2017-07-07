package streaming_jobs.noc_jobs

import core.streaming.NocParserBroadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods.parse
import parser.{NocLogLineObject, NocParser}
import storage.es.ElasticSearchDStreamWriter._
import org.apache.spark.sql.functions._
import scala.concurrent.duration.FiniteDuration
import org.apache.spark.sql.cassandra._
/**
  * Created by hungdv on 06/07/2017.
  */
object ParseAndSaveNoc {

  def parseAndSave(ssc: StreamingContext,ss: SparkSession,kafkaMessages: DStream[String],nocParser: NocParser): Unit ={
    import scala.language.implicitConversions
    implicit def finiteDurationToSparkDuration(value: FiniteDuration): org.apache.spark.streaming.Duration =
      new Duration(value.toMillis)
    import ss.implicits._
    val sc = ss.sparkContext
    val bParser = NocParserBroadcast.getInstance(sc,nocParser)
    val lines: DStream[NocLogLineObject] = kafkaMessages.transform(extractMessageAndValue("message", bParser))

    lines.persistToStorageDaily(Predef.Map[String, String]("indexPrefix" -> "noc", "type" -> "parsed"))

    val result  = lines.foreachRDD{
      (rdd: RDD[NocLogLineObject],time: org.apache.spark.streaming.Time) =>
        //filter from RDD -> sev == wraning or error
        rdd.cache()
        val brasErAndW: DataFrame = rdd.filter(line => (line.severity == "warning" || line.severity == "err"))
          .toDF("error","pri","devide","time","facility","severity")
          .select("devide","severity").cache()
        val brasErAndWaWithFlag= brasErAndW.withColumn("erro_flag",when(col("severity") === "err",1).otherwise(0))
                                           .withColumn("warning_flag",when(col("severity") === "warning",1).otherwise(0))
                                              .cache()

        val brasErrorCount = brasErAndWaWithFlag.groupBy(col("devide"))
          .agg(sum(col("erro_flag")).as("total_err_count"),sum(col("warning_flag")).as("total_warning_count"))

        brasErrorCount.write.mode("append").cassandraFormat("brasNocErrorCount","radius","test").save()
        rdd.unpersist(true)
        brasErAndW.unpersist(true)
        brasErAndWaWithFlag.unpersist(true)
    }

    ///////

  }
  def extractMessageAndValue = (key: String, bParser: Broadcast[NocParser]) => (mesgs: RDD[String]) => mesgs.map { msg =>
    implicit val formats = org.json4s.DefaultFormats
    val value = parse(msg).extract[Map[String, Any]].get(key).getOrElse(null)
    value.asInstanceOf[String]
  }.filter(value => value != null).map { line =>
    val parserObject = bParser.value.extractValue(line.replace("\n", "").replace("\r", "")).getOrElse(None)
    parserObject match {
      case Some(x) => x
      case _ => None
    }
    parserObject
  }.filter(x => x != None).map(ob => ob.asInstanceOf[parser.NocLogLineObject])

}
