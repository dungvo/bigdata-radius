package streaming_jobs.inf_jobs

import java.sql.Timestamp

import core.streaming.InfParserBroadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods.parse
import parser.{INFLogParser, InfLogLineObject}
import storage.es.ElasticSearchDStreamWriter._
import org.apache.spark.sql.cassandra._
/**
  * Created by hungdv on 19/06/2017.
  */
object ParseAndSaveInf {
  def parseAndSave(ssc: StreamingContext,
                   ss: SparkSession,
                   kafkaMessages: DStream[String],
                   infParser: INFLogParser): Unit = {
    val sc = ss.sparkContext
    val bParser = InfParserBroadcast.getInstance(sc, infParser)
    val bErrorType = sc.broadcast(Seq("module/cpe error","disconnect/lost IP"))
    import ss.implicits._
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val lines = kafkaMessages.transform(extractMessageAndValue("message", bParser))
    //TODO: Save to postgres.
    // DStreamToPostgres
    //lines.persistToStorageDaily(Predef.Map[String, String]("indexPrefix" -> "inf", "type" -> "rawLog"))
    val filtered = lines.filter(ob => (ob.logType == "module/cpe error" || ob.logType == "disconnect/lost IP" || ob.logType == "power off"))
    val mappedLogType = filtered.map{ob  =>
      var logType = ob.logType
      var count   = 1
      if(ob.logType == "power off"){
        logType = "disconnect/lost IP"
        count = -1
      }

      ((ob.hostName,logType,ob.time.substring(0,5)),count)
    }
    val hostErrorCounting = mappedLogType
       // .reduceByKey(_ + _)
      .reduceByKeyAndWindow(_ + _, _ - _,Duration(90*1000),Duration(30*1000))
      .filter(pair => (pair._2 > 0))
    val df = hostErrorCounting.map{
      value =>
        val result = new HostErrorCountObject(value._1._1,value._1._2,new Timestamp(System.currentTimeMillis()),value._2)
        //val result = new HostErrorCountObject(value._1._1,value._1._2,value._1._3,value._2)
        result
    }
    df.foreachRDD{
      //(rdd,time: Time) =>
        rdd =>
        val df = rdd.toDF("host","erro","time","count")
        val dfPivot = df.groupBy("host","time").pivot("erro",bErrorType.value)
                                              .agg(expr("coalesce(first(count),0)")).na.fill(0)
                                              //.cache()
                                              .withColumnRenamed("module/cpe error","cpe_error")
                                              .withColumnRenamed("disconnect/lost IP","lostip_error")
        //println("Time : " + time + "rdd - id" + rdd.id)
          //TODO : Save To Postgres.
        //dfPivot.show()
        dfPivot.write.mode("append").cassandraFormat("inf_host_error_counting","radius","test").save()
        //dfPivot.unpersist(true)
    }



    /*lines.foreachRDD{
      (rdd: RDD[InfLogLineObject],time: Time) =>

    }*/
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////// THE SAME WAY AS ABOVE
    /*lines.persistToStorageDaily(Predef.Map[String,String]("indexPrefix" -> "inf-parsed-test","type" -> "rawLog"))
    val lines = kafkaMessages.transform(extractMessage("message")
    val objectINFLogs: DStream[InfLogLineObject] = lines.transform(extractValue(bParser))
    objectINFLogs.persistToStorageDaily(Predef.Map[String,String]("indexPrefix" -> "inf-parsed","type" -> "rawLog"))*/
  }

  /**
    * Extract a field from Json Message.
    * @param jsonStr
    * @return
    */
  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, Any]]
  }

  /**
    * Extract message field from kafka message {"message":"content....."}
    * transformFunctionName = (params) => [(Source) => a tranformation]
    */
  def extractMessage = (key: String) => (mesgs: RDD[String]) => mesgs.map {
    msg =>
      implicit val formats = org.json4s.DefaultFormats
      val value = parse(msg).extract[Map[String, Any]].get(key).getOrElse(null)
      value.asInstanceOf[String]
  }.filter(value => value != null)

  def extractMessageAndValue = (key: String, bParser: Broadcast[INFLogParser]) => (mesgs: RDD[String]) => mesgs.map { msg =>
    implicit val formats = org.json4s.DefaultFormats
    val value = parse(msg).extract[Map[String, Any]].get(key).getOrElse(null)
    value.asInstanceOf[String]
  }.filter(value => value != null).map { line =>
    val parserObject = bParser.value.extractValues(line.replace("\n", "").replace("\r", "")).getOrElse(None)
    parserObject match {
      case Some(x) => x
      case _ => None
    }
    parserObject
    // NOTICE !!!!!!
    //println("1-"+line.replaceAll("^\\s|\n\\s|\\s$", "") + "-END")
    //println("2-"+line.trim().replaceAll("\n ", "") + "-END")
    //println("3-"+line.replace("\n", "").replace("\r", "") + "-END")
  }.filter(x => x != None).map(ob => ob.asInstanceOf[InfLogLineObject])


  def extractValue = (bParser: Broadcast[INFLogParser]) => (lines: RDD[String]) =>
    lines
      .map { line =>
        val parserObject = bParser.value.extractValues(line).getOrElse(None)
        parserObject match {
          case Some(x) => x
          //case Some(x) => x.asInstanceOf[parser.InfLogLineObject]
          case _ => None
        }
        parserObject
      }.filter(x => x != None).map(ob => ob.asInstanceOf[InfLogLineObject])
}

case class HostErrorCountObject(hostName: String,error: String,time: Timestamp,count: Int) extends Serializable{}