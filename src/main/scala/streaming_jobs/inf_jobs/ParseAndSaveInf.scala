package streaming_jobs.inf_jobs

import java.sql.Timestamp
import java.util.Properties

import core.streaming.InfParserBroadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods.parse
import parser.{INFLogParser, InfLogLineObject}
import storage.es.ElasticSearchDStreamWriter._
import org.apache.spark.sql.cassandra._
import storage.postgres.PostgresIO

/**
  * Created by hungdv on 19/06/2017.
  */
object ParseAndSaveInf {
  def parseAndSave(ssc: StreamingContext,
                   ss: SparkSession,
                   kafkaMessages: DStream[String],
                   infParser: INFLogParser,
                   postgresConfig: Map[String,String]): Unit = {
    val sc = ss.sparkContext
    val bParser = InfParserBroadcast.getInstance(sc, infParser)
    val bErrorType = sc.broadcast(Seq("module/cpe error", "disconnect/lost IP"))

    val jdbcUrl = PostgresIO.getJDBCUrl(postgresConfig)

    println(jdbcUrl)
    val bJdbcURL = sc.broadcast(jdbcUrl)
    //FIXME :
    // Ad-hoc fixing
    val pgProperties    = new Properties()
    pgProperties.setProperty("driver","org.postgresql.Driver")
    val bPgProperties   = sc.broadcast(pgProperties)
    import ss.implicits._
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val lines = kafkaMessages.transform(extractMessageAndValue("message", bParser)).cache()

    lines.foreachRDD{rdd =>
      val inf_df = rdd.toDF("log_type","host","date","time","module_ol")

      inf_df.createOrReplaceTempView("inf_df")
      import org.apache.spark.sql.functions.unix_timestamp

      val ts = unix_timestamp($"times_stamp_tmp", "MM/dd/yyyy HH:mm:ss").cast("timestamp")

      val inf_trf = ss.sql("select log_type,host,module_ol, concat(date ,' ', time) as times_stamp_tmp, concat(host,'/',module_ol) as module FROM inf_df")
                        .withColumn("times_stamp",ts)
                          .drop("times_stamp_tmp")

      PostgresIO.writeToPostgres(ss, inf_trf, bJdbcURL.value, "inf_error", SaveMode.Append, bPgProperties.value)
    }




    //TODO: Save to postgres.
    // DStreamToPostgres
    //lines.persistToStorageDaily(Predef.Map[String, String]("indexPrefix" -> "inf", "type" -> "rawLog"))
    val filtered = lines.filter(ob => (ob.logType == "module/cpe error" ||
                                        ob.logType == "disconnect/lost IP" ||
                                        ob.logType == "power off"))
    val mappedLogType = filtered.map { ob =>
      var logType = ob.logType
      var count = 1
      if (ob.logType == "power off") {
        logType = "disconnect/lost IP"
        count = -1
      }
      val key = ob.hostName + "/" + ob.module.trim //key = MHG103GAd/1/2/3 [host][module]
      ((key, logType, ob.time.substring(0, 5)), count)
    }
    val hostErrorCounting = mappedLogType
      // .reduceByKey(_ + _)
      .reduceByKeyAndWindow(_ + _, _ - _, Duration(90 * 1000), Duration(30 * 1000))
      .filter(pair => (pair._2 > 0))
    val ds: DStream[HostErrorCountObject] = hostErrorCounting.map {
      value =>
        val result = new HostErrorCountObject(value._1._1, value._1._2, new Timestamp(System.currentTimeMillis()), value._2)
        //val result = new HostErrorCountObject(value._1._1,value._1._2,value._1._3,value._2)
        result
    }

    ds.foreachRDD {
      //(rdd,time: Time) =>
      rdd =>
        val df = rdd.toDF("host_endpoint", "erro", "date_time", "count")
        val dfPivot = df.groupBy("host_endpoint", "date_time").pivot("erro", bErrorType.value)
          .agg(expr("coalesce(first(count),0)")).na.fill(0)
          //.cache()
          .withColumnRenamed("module/cpe error", "cpe_error")
          .withColumnRenamed("disconnect/lost IP", "lostip_error")

        dfPivot.createOrReplaceTempView("dfPivot")
        val result_inf_tmp = ss.sql("SELECT *,split(host_endpoint, '/')[0] as host," +
          "split(host_endpoint, '/')[2] as module_ol," +
          "split(host_endpoint, '/')[3] as index_ol FROM dfPivot").cache()
        //println("Time : " + time + "rdd - id" + rdd.id)
        //TODO : Save To Postgres.
        PostgresIO.writeToPostgres(ss, result_inf_tmp, bJdbcURL.value, "result_inf_tmp", SaveMode.Overwrite, bPgProperties.value)

        val host_endpoint_id_df = result_inf_tmp.select("host_endpoint")
        val host_endpoint_ids = host_endpoint_id_df.rdd.map(r => r(0)).collect()

        if (host_endpoint_ids.length > 0) {
          var host_endpoint_IdsString = "("
          host_endpoint_ids.foreach { x =>
            val y = "'" + x + "',"
            host_endpoint_IdsString = host_endpoint_IdsString + y
          }
          host_endpoint_IdsString = host_endpoint_IdsString.dropRight(1) + ")"
          val query = "insert into dwh_inf_index(bras_id,host_endpoint,host,module_ol,index,cpe_error,lostip_error,date_time)" +
            " select bh.bras_id,i.host_endpoint,i.host,i.module_ol,i.index_ol,i.cpe_error,i.lostip_error,i.date_time from result_inf_tmp i join " +
            "(SELECT * FROM brashostmapping WHERE host_endpoint in "+host_endpoint_IdsString+ " ) bh on i.host_endpoint = bh.host_endpoint "
          println(query )
          PostgresIO.pushDowmQuery("insert into dwh_inf_index(bras_id,host_endpoint,host,module_ol,index,cpe_error,lostip_error,date_time)" +
            " select bh.bras_id,i.host_endpoint,i.host,i.module_ol,i.index_ol,i.cpe_error,i.lostip_error,i.date_time from result_inf_tmp i join " +
            "(SELECT * FROM brashostmapping WHERE host_endpoint in "+host_endpoint_IdsString+ " ) bh on i.host_endpoint = bh.host_endpoint ",bJdbcURL.value)
        }

        result_inf_tmp.unpersist()
        //Save to cassandra
        //dfPivot.show()
        //dfPivot.write.mode("append").cassandraFormat("inf_host_error_counting", "radius", "test").save()
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
    *
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

case class HostErrorCountObject(hostName: String, error: String, time: Timestamp, count: Int) extends Serializable {}