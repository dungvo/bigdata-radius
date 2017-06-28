package streaming_jobs.inf_jobs

import core.streaming.InfParserBroadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods.parse
import parser.{INFLogParser, InfLogLineObject}
import storage.es.ElasticSearchDStreamWriter._

/**
  * Created by hungdv on 19/06/2017.
  */
object ParseAndSaveInf {
  def parseAndSave(ssc: StreamingContext,
                   ss: SparkSession,
                   kafkaMessages: DStream[String],
                   infParser: INFLogParser): Unit ={
    val sc = ss.sparkContext
    val bParser = InfParserBroadcast.getInstance(sc,infParser)

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val lines = kafkaMessages.transform(extractMessage("message",bParser))
                                                //.transform(extractValue(bParser))

    lines.foreachRDD{
     rdd => rdd.foreach{println(_)}
   }
    /*val objectINFLogs: DStream[InfLogLineObject] = lines.transform(extractValue(bParser))
    objectINFLogs.foreachRDD{rdd =>
      println("Count :" +  rdd.count())
      rdd.foreach(println(_))
    }*/

    //kafkaMessages.persistToStorageDaily(Predef.Map[String,String]("indexPrefix" -> "inf-raw-test","type" -> "rawLog"))
/*    val lines: DStream[String] = kafkaMessages.map{
      json =>
        val mapOfRawLogObject: Map[String, Any] = jsonStrToMap(json)
        val msg = mapOfRawLogObject.get("message").getOrElse(null)
        msg.asInstanceOf[String]
    }.filter(msg => msg!= null)
    lines.persistToStorageDaily(Predef.Map[String,String]("indexPrefix" -> "inf-parsed-test","type" -> "rawLog"))

    val objectINFLogs: DStream[InfLogLineObject] = lines.transform(extractValue(bParser))
    objectINFLogs.persistToStorageDaily(Predef.Map[String,String]("indexPrefix" -> "inf-parsed","type" -> "rawLog"))*/
    //lines.persistToStorageDaily(Predef.Map[String,String]("indexPrefix" -> "inf-test","type" -> "rawLog"))
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /*  kafkaMessages.foreachRDD{
      rdd => {
        rdd.map{
          json =>
            val mapOfRawLogObject: Map[String, Any] = jsonStrToMap(json)
            val msg = mapOfRawLogObject.get("message").getOrElse(null)
            msg.asInstanceOf[String]
        }.filter(msg => msg!= null)
          .map{ line =>
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
    }*/

  }
  def jsonStrToMap(jsonStr: String) :Map[String,Any] ={

    implicit val formats = org.json4s.DefaultFormats

    parse(jsonStr).extract[Map[String,Any]]

  }
  /**
    * Extract message field from kafka message {"message":"content....."}
    * transformFunctionName = (params) => [(Source) => a tranformation]
   */
/*  def extractMessage = (key: String) => (mesgs: RDD[String]) => mesgs.map{
    msg =>
      implicit val formats = org.json4s.DefaultFormats
      val value = parse(msg).extract[Map[String,Any]].get(key).getOrElse(null)
      value.asInstanceOf[String]
  }.filter(value => value != null)*/
def extractMessage = (key: String,bParser: Broadcast[INFLogParser]) => (mesgs: RDD[String]) => mesgs.map{
  msg =>
    implicit val formats = org.json4s.DefaultFormats
    val value = parse(msg).extract[Map[String,Any]].get(key).getOrElse(null)
    value.asInstanceOf[String]
}.filter(value => value != null)
  .map{ line =>
  val parserObject = bParser.value.extractValues(line).getOrElse(None)
  parserObject match{
    case Some(x) => x
    case _ => None
  }
  parserObject
}.filter(x => x!= None).map(ob => ob.asInstanceOf[InfLogLineObject])




  def extractValue = (bParser: Broadcast[INFLogParser]) => (lines: RDD[String]) =>
    lines
/*      .map{ json =>
          val mapOfRawLogObject: Map[String, Any] = jsonStrToMap(json)
          val msg = mapOfRawLogObject.get("message").getOrElse(null)
          msg.asInstanceOf[String]
         }.filter(msg => msg!= null)*/
      .map{ line =>
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

