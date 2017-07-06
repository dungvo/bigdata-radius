package streaming_jobs.noc_jobs

import core.streaming.NocParserBroadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods.parse
import parser.{ NocParser,NocLogLineObject}
import storage.es.ElasticSearchDStreamWriter._

/**
  * Created by hungdv on 06/07/2017.
  */
object ParseAndSaveNoc {
  def parseAndSave(ssc: StreamingContext,ss: SparkSession,kafkaMessages: DStream[String],nocParser: NocParser): Unit ={
    val sc = ss.sparkContext
    val bParser = NocParserBroadcast.getInstance(sc,nocParser)
    val lines = kafkaMessages.transform(extractMessageAndValue("message", bParser))
    lines.persistToStorageDaily(Predef.Map[String, String]("indexPrefix" -> "noc", "type" -> "parsed"))
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
