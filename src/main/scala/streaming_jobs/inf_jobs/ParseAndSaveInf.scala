package streaming_jobs.inf_jobs

import core.streaming.InfParserBroadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import parser.{INFLogParser, InfLogLineObject}
import storage.es.ElasticSearchDStreamWriter._

/**
  * Created by hungdv on 19/06/2017.
  */
object ParseAndSave {
  def parseAndSave(ssc: StreamingContext,
                   ss: SparkSession,
                   lines: DStream[String],
                   infParser: INFLogParser): Unit ={
    val sc = ss.sparkContext
    val bParser = InfParserBroadcast.getInstance(sc,infParser)
    val objectINFLogs = lines.transform(extractValue(bParser))
    objectINFLogs.persistToStorageDaily(Predef.Map[String,String]("indexPrefix" -> "inf","type" -> "rawLog"))

  }
  def extractValue = (bParser: Broadcast[INFLogParser]) => (lines: RDD[String]) =>
    lines.map{
      line =>
      val parserObject = bParser.value.extractValues(line).getOrElse(None)
      parserObject match{
        case Some(x) => x
        //case Some(x) => x.asInstanceOf[parser.InfLogLineObject]
        case _ => None
      }
        //System.out.println("parsed : " + line)
      parserObject
    }.filter(x => x!= None).map(ob => ob.asInstanceOf[InfLogLineObject])
}
