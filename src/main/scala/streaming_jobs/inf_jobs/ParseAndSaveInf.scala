package streaming_jobs.inf_jobs

import java.sql.{Connection, DriverManager, SQLException, Timestamp}
import java.util.{Properties, UUID}

import core.streaming.{InfParserBroadcast, RedisClusterClientFactory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, expr, from_unixtime}
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods.parse
import parser.{INFLogParser, InfLogLineObject}
import storage.es.ElasticSearchDStreamWriter._
import org.apache.spark.sql.cassandra._
import storage.postgres.PostgresIO
import org.apache.spark.TaskContext

import scala.concurrent.duration.{Duration, SECONDS}
import java.util.concurrent.Executors

import core.KafkaProducerFactory
import core.sinks.KafkaDStreamSinkExceptionHandler
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.{Await, ExecutionContext, Future}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import util.DatetimeController
/**
  * Created by hungdv on 19/06/2017.
  */
object ParseAndSaveInf {
  def parseAndSave(ssc: StreamingContext,
                   ss: SparkSession,
                   kafkaMessages: DStream[String],
                   infParser: INFLogParser,
                   postgresConfig: Map[String,String],
                   infPortDownTopic: String,
                   producerConfig: Map[String,String]): Unit = {
    val sc = ss.sparkContext
    val bParser = InfParserBroadcast.getInstance(sc, infParser)
    val bErrorType = sc.broadcast(Seq("module/cpe error", "disconnect/lost IP"))
    val bInfPortDown = sc.broadcast(infPortDownTopic)
    val bInfLos = sc.broadcast("inf-los-detect")
    val bProducerConfig = sc.broadcast[Map[String,String]](producerConfig)
    val jdbcUrl = PostgresIO.getJDBCUrl(postgresConfig)

    //println("START INF JOB")

    //println(jdbcUrl)
    val bJdbcURL = sc.broadcast(jdbcUrl)
    //FIXME :
    // Ad-hoc fixing
    val pgProperties    = new Properties()
    pgProperties.setProperty("driver","org.postgresql.Driver")
    val bPgProperties   = sc.broadcast(pgProperties)
    import ss.implicits._
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val lines = kafkaMessages.transform(extractMessageAndValue("message", bParser)).cache()

/*
    try{
      import storage.es.ElasticSearchDStreamWriter._
      // today just evaluate when called the first time.
      //var today = org.joda.time.DateTime.now().toString("yyyy-MM-dd")
      //def today = org.joda.time.DateTime.now().toString("yyyy-MM-dd")
      // Tip 1 : Use def instead
      //Save conn log to ES
      // Tip 2 : do not declare variable.
      // Notice var x: Int ---- var x: => Int


      //objectConnLogs.persistToStorageDaily(Predef.Map[String,String]("indexPrefix" -> "radius-connlog_new","type" -> "connlog"))
      lines.persistToStorageDaily(Predef.Map[String,String]("indexPrefix" -> "inf" ,"type" -> "inf_erro"))
      //lines.persistToStorage(Predef.Map[String,String]("index" -> ("inf-" + org.joda.time.DateTime.now().toString("yyyy-MM-dd_HH:mm:ss")),"type" -> "inf_erro"))
      //objectConnLogs.persistToStorage(Predef.Map[String,String]("index" -> ("radius-test-" + today),"type" -> "connlog"))
    } catch {
        case e: Exception => System.err.println("UncatchException occur when save inf log to ES : " +  e.getMessage)
        case _ => println("Ignore !")
    }
*/


    lines.foreachRDD{rdd =>
      val inf_df = rdd.toDF("log_type","host","date","time","module_ol")
      inf_df.cache()
      inf_df.createOrReplaceTempView("inf_df")
      //inf_df.show(10)
      import org.apache.spark.sql.functions.unix_timestamp

      val ts = unix_timestamp($"times_stamp_tmp", "yyyy/MM/dd HH:mm:ss").cast("timestamp")

      val inf_trf = ss.sql("select log_type,host,module_ol, concat(date ,' ', time) as times_stamp_tmp, concat(host,'/',trim(module_ol)) as module FROM inf_df")
                        .withColumn("times_stamp",ts)
                        .drop("times_stamp_tmp")
      inf_trf.createOrReplaceTempView("inf_trf")
      //inf_trf.show(10)
      //TEST
      try{
        PostgresIO.writeToPostgres(ss, inf_trf, bJdbcURL.value, "inf_error", SaveMode.Append, bPgProperties.value)
      }catch{
        case e: SQLException => System.err.println("SQLException occur when save inf_error : " + e.getSQLState + " " + e.getMessage)
        case e: Exception => System.err.println("UncatchException occur when save inf_error : " +  e.getMessage)
        case _ => println("Ignore !")
      }

      val filterdForPortDown = ss.sql("SELECT log_type, times_stamp, module FROM  inf_trf WHERE log_type = 'module/cpe error' OR log_type = 'disconnect/lost IP' OR log_type = 'user port down' OR  log_type = 'power off' ")

      filterdForPortDown.rdd.foreachPartition{partition =>
        if(partition.hasNext){
          val producer: KafkaProducer[String,String] = KafkaProducerFactory.getOrCreateProducer(bProducerConfig.value)
          val context = TaskContext.get()
          val callback = new KafkaDStreamSinkExceptionHandler
          //val logger = Logger.getLogger(getClass)
          //logger.debug(s"Send Spark partition: ${context.partitionId()} to Kafka topic in [anomaly]")
          partition.map{row =>
            // log_type:times_stamp:module
            val massage = row.getAs[String]("log_type")+"#"+row.getAs[java.sql.Timestamp]("times_stamp").toString+"#"+row.getAs[String]("module")
            //println(massage)
            //val record = new ProducerRecord[String,String](bAnomalyDetectKafkaTopic.value,"anomaly",string)
            //Hope this will help.

            val record = new ProducerRecord[String,String](bInfPortDown.value,UUID.randomUUID().toString,massage)
            callback.throwExceptionIfAny()
            producer.send(record,callback)
          }.toList
        }
      }
      // LOS
      val converFlag: (String => Int) = (args: String)=> {
        if(args == "los" || args == "disconnect/lost IP") 1 else if(args == "power off" || args == "register") -1 else -1000
      }
      val sqlFlag = org.apache.spark.sql.functions.udf(converFlag)
      val filterdLos = ss.sql("SELECT concat(split(module, '/')[0],'/',split(module, '/')[2]) as module_ol,split(module, '/')[3] as index_ol,times_stamp,log_type   FROM  inf_trf" +
        " WHERE log_type = 'los' OR log_type = 'power off' OR log_type = 'user port down' OR log_type = 'disconnect/lost IP' OR log_type = 'register' ")
                            .withColumn("times_stamp",from_unixtime(unix_timestamp(col("times_stamp"),"yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd HH:mm"))
        .withColumn("flag",sqlFlag(col("log_type")))
      println("negative")
      //filterdLos.where("flag < 0").show(100)
      println("==================== END NEGATIVE")


      println("====== REMOVE LINKDOWN")
      val linkdow = filterdLos.where($"index_ol".isNull).select("module_ol").withColumn("flagLinkDown",lit("1"))
      //linkdow.show()
      linkdow.createOrReplaceTempView("linkDown")
      val joinedLinkdown = filterdLos.join(linkdow,Seq("module_ol"),"left_outer")
      //joinedLinkdown.show()
      //debug

      val removedLinkdown = joinedLinkdown.where($"flagLinkDown".isNull ).drop(col("flagLinkDown"))

      removedLinkdown.createOrReplaceTempView("los_and_po")
      //filterdLos.show(100)
      val filterLOSandPO = ss.sql("SELECT module_ol,index_ol,times_stamp,Sum(flag) as sum FROM los_and_po GROUP BY module_ol,index_ol,times_stamp HAVING Sum(flag) > 0 ")
      println("filterLOSandPO")
      //filterLOSandPO.show(100)
      println("============== END filtered")
      inf_df.unpersist()
      // Send LOS to kafka2
      filterLOSandPO.rdd.foreachPartition{
        partition =>
          if(partition.hasNext){
            val producer: KafkaProducer[String,String] = KafkaProducerFactory.getOrCreateProducer(bProducerConfig.value)
            val context = TaskContext.get()
            val callback = new KafkaDStreamSinkExceptionHandler
            partition.map{
              row =>
                val massage = row.getAs[String]("times_stamp").toString+"#"+row.getAs[String]("module_ol")+"#"+row.getAs[String]("index_ol")
                //val massage = row.getAs[java.sql.Timestamp]("times_stamp").toString+"#"+row.getAs[String]("module_ol")+"#"+row.getAs[String]("index_ol")
                println(massage)
                //val record = new ProducerRecord[String,String](bAnomalyDetectKafkaTopic.value,"anomaly",string)
                //Hope this will help.
                val record = new ProducerRecord[String,String](bInfLos.value,UUID.randomUUID().toString,massage)
                callback.throwExceptionIfAny()
                producer.send(record,callback)
            }.toList
          }
      }
    }
    //val filteredForPortDown = lines.filter(ob => (ob.logType == "disconnect/lost IP" || ob.logType == "user port down" || ob.logType == "module/cpe error"))
    //TODO: Save to postgres.
    // DStreamToPostgres
    //lines.persistToStorageDaily(Predef.Map[String, String]("indexPrefix" -> "inf", "type" -> "rawLog"))
    val filtered: DStream[InfLogLineObject] = lines.filter(ob => (ob.logType == "module/cpe error" ||
                                        ob.logType == "disconnect/lost IP" ||
                                        ob.logType == "power off" ||
                                        ob.logType == "los"))
    //filteredForPortDown.foreachRDD{
    //  rdd =>
    //    val df = rdd.toDF()
    //}

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
      .reduceByKeyAndWindow(_ + _, _ - _, org.apache.spark.streaming.Duration(120 * 1000),org.apache.spark.streaming.Duration(120 * 1000))
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
        val df = rdd.toDF("host_endpoint", "erro", "date_time", "count").cache

        // should i cache it

        val dfPivot = df.filter("erro != 'los'")
          .groupBy("host_endpoint", "date_time").pivot("erro", bErrorType.value)
          .agg(expr("coalesce(first(count),0)")).na.fill(0)
          //.cache()
          .withColumnRenamed("module/cpe error", "cpe_error")
          .withColumnRenamed("disconnect/lost IP", "lostip_error")
        //dfPivot.show(10)
        dfPivot.createOrReplaceTempView("dfPivot")
        val result_inf_tmp = ss.sql("SELECT *,split(host_endpoint, '/')[0] as host," +
          "split(host_endpoint, '/')[2] as module_ol," +
          "split(host_endpoint, '/')[3] as index_ol FROM dfPivot").cache()
        //result_inf_tmp.show(10)
        //println("Time : " + time + "rdd - id" + rdd.id)
        //TODO : Save To Postgres.

        try{
          PostgresIO.writeToPostgres(ss, result_inf_tmp, bJdbcURL.value, "result_inf_tmp", SaveMode.Overwrite, bPgProperties.value)
        }catch{
          case e: SQLException => System.err.println("SQLException occur when save result_inf_tmp" + e.getSQLState + " " + e.getMessage)
          case e: Exception => System.err.println("UncatchException occur when save result_inf_tmp: " +  e.getMessage)
          case _ => println("Ignore !")
        }
        df.unpersist()
        val host_endpoint_id_df = result_inf_tmp.select("host_endpoint")
        //host_endpoint_id_df.show()
        val host_endpoint_ids: Array[Any] = host_endpoint_id_df.rdd.map(r => r(0)).collect()

        if (host_endpoint_ids.length > 0) {
          var host_endpoint_IdsString = "("
          host_endpoint_ids.foreach { x =>
            val y = "'" + x + "',"
            host_endpoint_IdsString = host_endpoint_IdsString + y
          }
          host_endpoint_IdsString = host_endpoint_IdsString.dropRight(1) + ")"


          val insertINFIndexQuery = "insert into dwh_inf_index(bras_id,host_endpoint,host,module_ol,index,cpe_error,lostip_error,date_time)" +
            " select bh.bras_id,i.host_endpoint,i.host,i.module_ol,i.index_ol,i.cpe_error,i.lostip_error,i.date_time from result_inf_tmp i join " +
            "(SELECT * FROM brashostmapping WHERE host_endpoint in "+host_endpoint_IdsString+ " ) bh on i.host_endpoint = bh.host_endpoint "

          //println(insertINFIndexQuery)
          PostgresIO.pushDownJDBCQuery(insertINFIndexQuery,bJdbcURL.value)
          //

          val insertINFModuleQuery = "insert into dwh_inf_module(bras_id,host,module,cpe_error,lostip_error,date_time) " +
            "select bh.bras_id,i.host,i.module_ol,SUM(i.cpe_error), SUM(i.lostip_error),i.date_time " +
            "from result_inf_tmp i join (SELECT * FROM brashostmapping WHERE host_endpoint in " + host_endpoint_IdsString +
            ") bh on i.host_endpoint = bh.host_endpoint GROUP BY i.host,bh.bras_id,i.date_time,i.module_ol ;"
          //println(insertINFModuleQuery)

          val insertINFHostQuery = "insert into dwh_inf_host(bras_id,host,cpe_error,lostip_error,date_time) " +
              "select bh.bras_id,i.host,SUM(i.cpe_error),SUM(i.lostip_error),i.date_time " +
              "from result_inf_tmp i join (SELECT * FROM brashostmapping WHERE host_endpoint in " + host_endpoint_IdsString +
            ") bh on i.host_endpoint = bh.host_endpoint GROUP BY i.host,bh.bras_id,i.date_time ;"
          //println(insertINFHostQuery)



            // CO VE KHONG OK LAM, JOB DIE WITHOUT ERROR CODE AFTER 14 DAYS.
          // Back to normal.

          /*// Set number of threads via a configuration property
          val pool = Executors.newFixedThreadPool(3)
          // create the implicit ExecutionContext based on our thread pool
          implicit val xc = ExecutionContext.fromExecutorService(pool)
          // create two async task.
          try{
            val insertINFHostTask = PostgresIO.pushDownQueryAsync(insertINFHostQuery,bJdbcURL.value)
            val insertINFModuleTask = PostgresIO.pushDownQueryAsync(insertINFModuleQuery,bJdbcURL.value)
            Await.result(Future.sequence(Seq(insertINFHostTask,insertINFModuleTask)), scala.concurrent.duration.Duration(5,SECONDS))
          }catch{
            case e: SQLException => System.err.println("SQLException occur when save host-module : " + e.getSQLState + " " + e.getMessage)
            case e: Exception => System.err.println("UncatchException occur when save host-module : " +  e.getMessage)
            case _ => println("Ignore !")
          }*/

          try{
            PostgresIO.pushDownJDBCQuery(insertINFHostQuery,bJdbcURL.value)
            PostgresIO.pushDownJDBCQuery(insertINFModuleQuery,bJdbcURL.value)

          }catch{
            case e: SQLException => System.err.println("SQLException occur when save host-module : " + e.getSQLState + " " + e.getMessage)
            case e: Exception => System.err.println("UncatchException occur when save host-module : " +  e.getMessage)
            case _ => println("Ignore !")
          }


          //Await.result(Future.sequence(Seq(taskA,taskB)), Duration(1, MINUTES))
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
  def pushDownQueryAsync(query: String,url: String)(implicit xc: ExecutionContext) = Future {
    PostgresIO.pushDownJDBCQuery(query,url)
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
    //.filter(x => x.hostName != "n/a")


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

object ParseAndSaveInfV2 {
  def parseAndSave(ssc: StreamingContext,
                   ss: SparkSession,
                   kafkaMessages: DStream[String],
                   infParser: INFLogParser,
                   postgresConfig: Map[String,String],
                   infPortDownTopic: String,
                   producerConfig: Map[String,String],
                   redisNodes: String): Unit = {
    val sc = ss.sparkContext
    val bParser = InfParserBroadcast.getInstance(sc, infParser)
    val bErrorType = sc.broadcast(Seq("module/cpe error", "disconnect/lost IP"))
    val bInfPortDown = sc.broadcast(infPortDownTopic)
    val bInfLos = sc.broadcast("inf-los-detect")
    val bProducerConfig = sc.broadcast[Map[String,String]](producerConfig)
    val jdbcUrl = PostgresIO.getJDBCUrl(postgresConfig)

    //println("START INF JOB")

    //println(jdbcUrl)
    val bJdbcURL = sc.broadcast(jdbcUrl)
    //FIXME :
    // Ad-hoc fixing
    val pgProperties    = new Properties()
    pgProperties.setProperty("driver","org.postgresql.Driver")
    val bPgProperties   = sc.broadcast(pgProperties)
    import ss.implicits._
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val lines = kafkaMessages.transform(extractMessageAndValue("message", bParser)).cache()

/*    try{
      import storage.es.ElasticSearchDStreamWriter._
      // today just evaluate when called the first time.
      //var today = org.joda.time.DateTime.now().toString("yyyy-MM-dd")
      //def today = org.joda.time.DateTime.now().toString("yyyy-MM-dd")
      // Tip 1 : Use def instead
      //Save conn log to ES
      // Tip 2 : do not declare variable.
      // Notice var x: Int ---- var x: => Int


      //objectConnLogs.persistToStorageDaily(Predef.Map[String,String]("indexPrefix" -> "radius-connlog_new","type" -> "connlog"))
      lines.persistToStorageDaily(Predef.Map[String,String]("indexPrefix" -> "inf" ,"type" -> "inf_erro"))
      //lines.persistToStorage(Predef.Map[String,String]("index" -> ("inf-" + org.joda.time.DateTime.now().toString("yyyy-MM-dd_HH:mm:ss")),"type" -> "inf_erro"))
      //objectConnLogs.persistToStorage(Predef.Map[String,String]("index" -> ("radius-test-" + today),"type" -> "connlog"))
    } catch {
      case e: Exception => System.err.println("UncatchException occur when save inf log to ES : " +  e.getMessage)
      case _ => println("Ignore !")
    }*/


    lines.foreachRDD{rdd =>
      val inf_df = rdd.toDF("log_type","host","date","time","module_ol")
      inf_df.cache()
      inf_df.createOrReplaceTempView("inf_df")
      //inf_df.show(10)
      import org.apache.spark.sql.functions.unix_timestamp

      val ts = unix_timestamp($"times_stamp_tmp", "yyyy/MM/dd HH:mm:ss").cast("timestamp")

      val inf_trf = ss.sql("select log_type,host,module_ol, concat(date ,' ', time) as times_stamp_tmp, concat(host,'/',trim(module_ol)) as module FROM inf_df")
        .withColumn("times_stamp",ts)
        .drop("times_stamp_tmp")
      inf_trf.createOrReplaceTempView("inf_trf")
      //inf_trf.show(10)
      //TEST
      try{
        PostgresIO.writeToPostgres(ss, inf_trf, bJdbcURL.value, "inf_error", SaveMode.Append, bPgProperties.value)
      }catch{
        case e: SQLException => System.err.println("SQLException occur when save inf_error : " + e.getSQLState + " " + e.getMessage)
        case e: Exception => System.err.println("UncatchException occur when save inf_error : " +  e.getMessage)
        case _ => println("Ignore !")
      }

      val filterdForPortDown = ss.sql("SELECT log_type, times_stamp, module FROM  inf_trf WHERE log_type = 'module/cpe error' " +
        "OR log_type = 'disconnect/lost IP' " +
        "OR log_type = 'user port down' " +
        "OR  log_type = 'power off' ")

      filterdForPortDown.rdd.foreachPartition{partition =>
        if(partition.hasNext){
          val producer: KafkaProducer[String,String] = KafkaProducerFactory.getOrCreateProducer(bProducerConfig.value)
          val context = TaskContext.get()
          val callback = new KafkaDStreamSinkExceptionHandler
          //val logger = Logger.getLogger(getClass)
          //logger.debug(s"Send Spark partition: ${context.partitionId()} to Kafka topic in [anomaly]")
          partition.map{row =>
            // log_type:times_stamp:module
            val massage = row.getAs[String]("log_type")+"#"+row.getAs[java.sql.Timestamp]("times_stamp").toString+"#"+row.getAs[String]("module")
            //println(massage)
            //val record = new ProducerRecord[String,String](bAnomalyDetectKafkaTopic.value,"anomaly",string)
            //Hope this will help.

            val record = new ProducerRecord[String,String](bInfPortDown.value,UUID.randomUUID().toString,massage)
            callback.throwExceptionIfAny()
            producer.send(record,callback)
          }.toList
        }
      }
      // LOS


      val filterdLos = ss.sql("SELECT concat(split(module, '/')[0],'/',split(module, '/')[2]) as module_ol,split(module, '/')[3] as index_ol,times_stamp,log_type   FROM  inf_trf" +
        " WHERE log_type = 'los' OR log_type = 'power off' OR log_type = 'user port down' OR log_type = 'disconnect/lost IP' OR log_type = 'register' ")
        //.withColumn("times_stamp",from_unixtime(unix_timestamp(col("times_stamp"),"yyyy-MM-dd HH:mm:ss"),"yyyy-MM-dd HH:mm"))
        //.withColumn("times_stamp",$"times_stamp".cast(TimestampType))
        .withColumn("times_stamp",org.apache.spark.sql.functions.current_timestamp())
        .cache()

      //println("negative")
      //filterdLos.where("flag < 0").show(100)
      //println("==================== END NEGATIVE")
      val powerOffOrLinkDown = filterdLos.where($"log_type" === "power off" || $"log_type" === "user port down" || $"log_type" === "register" )
        //
      powerOffOrLinkDown.rdd.map{row =>
        try{
          val time = row.getAs[java.sql.Timestamp]("times_stamp")
          val event =  ModermEvent(
            row.getAs[String]("index_ol"),
            row.getAs[String]("log_type"),
            row.getAs[String]("module_ol"),
            time.getTime
          )
          event
        } catch{
          case e: Exception => {
            ModermEvent("n/a","","",0L)
          }
        }
      }.filter(x => x.hostName != "n/a").map{ x => (x.hostName,x)}
          // better using combineByKey()
        .groupByKey()
        .foreachPartition{part =>
        if(part.hasNext){
          //println("power off and linkdown - has next")
          val redisClient = RedisClusterClientFactory.getOrCreateClient(redisNodes)
          part.foreach{
          tuple =>
              tuple._2.toList.sortWith(_.time  < _.time).foreach{
                x => redisClient.set("dh_"+x.olt+"/"+x.hostName,x.error)
                     //println(" SET status : " + x.olt+"/"+x.hostName + " " + x.error)
              }
          }

        }
      }
      val powerOffOrLinkDownSelected = powerOffOrLinkDown.select("index_ol","log_type","module_ol","times_stamp")
                                                .where($"log_type" === "power off" || $"log_type" === "user port down" )
                                                .withColumn("server_time", org.apache.spark.sql.functions.current_timestamp())
                                                .drop("times_stamp")
                                                .withColumnRenamed("server_time","times_stamp")
                                                .na.fill("0")
                                                .dropDuplicates("index_ol","module_ol","times_stamp")



      try{
        PostgresIO.writeToPostgres(ss,powerOffOrLinkDownSelected,bJdbcURL.value, "devices_history", SaveMode.Append, bPgProperties.value)
        //PostgresIO.writeToPostgres(ss,powerOffOrLinkDownSelected,"meomeo", SaveMode.Append, bPgProperties.value)
      }catch{
        case e: SQLException => System.err.println("SQLException occur when save devices error : " + e.getSQLState + " " + e.getMessage)
        case e: Exception => System.err.println("UncatchException occur when save devices error : " +  e.getMessage)
        case _ => println("Ignore !")
      }

      // Save to postgres.
      // Update Redis Client.

      //println("====== REMOVE LINKDOWN")
      val linkdow = filterdLos.where($"index_ol".isNull).select("module_ol").withColumn("flagLinkDown",lit("1"))
      //linkdow.show()
      linkdow.createOrReplaceTempView("linkDown")
      val joinedLinkdown = filterdLos.join(linkdow,Seq("module_ol"),"left_outer")
      //joinedLinkdown.show()
      //debug
      val removedLinkdown = joinedLinkdown.where($"flagLinkDown".isNull ).drop(col("flagLinkDown"))

      //Check last status.
      val losOnly = removedLinkdown.where($"log_type" === "disconnect/lost IP"  || $"log_type" === "los")
        //.withColumn("times_stamp",$"times_stamp".cast(TimestampType))
      //
      val los = losOnly.rdd.map{ row =>
        try{
          val time = row.getAs[java.sql.Timestamp]("times_stamp")
          //println("time : " + time)
          val event =  ModermEvent2(
            row.getAs[String]("index_ol"),
            row.getAs[String]("log_type"),
            row.getAs[String]("module_ol"),
            time
          )
          event
        } catch{
          case e: Exception => {
            println("exception " + e.getStackTrace + " " + e.getMessage)
            ModermEvent2("n/a","","",new Timestamp(0L))
          }
        }
      }.filter(x => x.hostName != "n/a")
        .foreachPartition{part =>
          if(part.hasNext){
            //println("lost only - has next")
            val redisClient = RedisClusterClientFactory.getOrCreateClient(redisNodes)


            val filtered: Iterator[ModermEvent2] = part.filter{x =>
              val y = (redisClient.get("dh_"+x.olt+"/"+x.hostName))
              y == null
            }

            val producer: KafkaProducer[String,String] = KafkaProducerFactory.getOrCreateProducer(bProducerConfig.value)
            val context = TaskContext.get()
            val callback = new KafkaDStreamSinkExceptionHandler

            if(filtered.hasNext){
              filtered.map{
                //filtered.foreach{
                event =>
                  // It's massage man. mot messenge
                  val massage = event.time.toString +"#"+event.olt+"#"+event.hostName
                  println("KAFKA MESSAGE : " + massage)
                  //val massage = row.getAs[java.sql.Timestamp]("times_stamp").toString+"#"+row.getAs[String]("module_ol")+"#"+row.getAs[String]("index_ol")
                  //println(massage)
                  //val record = new ProducerRecord[String,String](bAnomalyDetectKafkaTopic.value,"anomaly",string)
                  //Hope this will help.
                  val record = new ProducerRecord[String,String](bInfLos.value,UUID.randomUUID().toString,massage)
                  callback.throwExceptionIfAny()
                  producer.send(record,callback)
              }.toList



              println("SAVE LOST TO POSTGRES")
              /*val seq: Seq[ModermEvent2] = filtered.toSeq
              seq.foreach{x => println(x)}
              val los_DF = seq.toDF("index_ol","log_type","module_ol","times_stamp")
              //val los_DF = ss.sparkContext.parallelize(seq).toDF("index_ol","log_type","module_ol","times_stamp")
              //val los_DF = ss.sparkContext.parallelize(filtered.toSeq).toDF("index_ol","log_type","module_ol","times_stamp")
              try{
                PostgresIO.writeToPostgres(ss,los_DF,bJdbcURL.value, "devices_history", SaveMode.Append, bPgProperties.value)
                //PostgresIO.writeToPostgres(ss,powerOffOrLinkDownSelected,"meomeo", SaveMode.Append, bPgProperties.value)
              }catch{
                case e: SQLException => System.err.println("SQLException occur when save devices error : " + e.getSQLState + " " + e.getMessage)
                case e: Exception => System.err.println("UncatchException occur when save devices error : " +  e.getMessage)
                case _ => println("Ignore !")
              }*/
              insertIteratorToPostgres(filtered,bJdbcURL.value)
            }




          }
        }


      //println("SET STATUS TO REDIS : ")
      filterdLos.rdd.map{ row =>
        try{
          val time = row.getAs[java.sql.Timestamp]("times_stamp")
          val event =  ModermEvent(
            row.getAs[String]("index_ol"),
            row.getAs[String]("log_type"),
            row.getAs[String]("module_ol"),
            time.getTime
          )
          event
        } catch{
          case e: Exception => {
            ModermEvent("n/a","","",0L)
          }
        }
      }.filter(x => x.hostName != "n/a")
        .map{ x => (x.hostName,x)}
        // better using combineByKey()
        .groupByKey()
        .foreachPartition { part =>
          if (part.hasNext) {
            val redisClient = RedisClusterClientFactory.getOrCreateClient(redisNodes)
            part.foreach{
              tuple => tuple._2.toList.sortWith(_.time  < _.time).foreach {event =>
                redisClient.set("hd_status_" + event.olt+"/"+event.hostName, event.error + "#" + event.time)
                // println("Set hd_status: " +  event.olt+"/"+event.hostName)
                redisClient.expire("hd_status_" + event.olt+"/"+event.hostName, 60 * 6)
              }
            }
          }
        }
    }
    //val filteredForPortDown = lines.filter(ob => (ob.logType == "disconnect/lost IP" || ob.logType == "user port down" || ob.logType == "module/cpe error"))
    //TODO: Save to postgres.
    // DStreamToPostgres
    //lines.persistToStorageDaily(Predef.Map[String, String]("indexPrefix" -> "inf", "type" -> "rawLog"))
    val filtered: DStream[InfLogLineObject] = lines.filter(ob => (ob.logType == "module/cpe error" ||
      ob.logType == "disconnect/lost IP" ||
      ob.logType == "power off" ||
      ob.logType == "los"))
    //filteredForPortDown.foreachRDD{
    //  rdd =>
    //    val df = rdd.toDF()
    //}

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
      .reduceByKeyAndWindow(_ + _, _ - _, org.apache.spark.streaming.Duration(120 * 1000),org.apache.spark.streaming.Duration(120 * 1000))
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
          .withColumnRenamed("date_time","device_time")
          .withColumn("date_time",org.apache.spark.sql.functions.current_timestamp())
          .cache

        // should i cache it

        val dfPivot = df.filter("erro != 'los'")
          .groupBy("host_endpoint", "date_time").pivot("erro", bErrorType.value)
          .agg(expr("coalesce(first(count),0)")).na.fill(0)
          //.cache()
          .withColumnRenamed("module/cpe error", "cpe_error")
          .withColumnRenamed("disconnect/lost IP", "lostip_error")
        //dfPivot.show(10)
        dfPivot.createOrReplaceTempView("dfPivot")
        val result_inf_tmp = ss.sql("SELECT *,split(host_endpoint, '/')[0] as host," +
          "split(host_endpoint, '/')[2] as module_ol," +
          "split(host_endpoint, '/')[3] as index_ol FROM dfPivot").cache()
        //result_inf_tmp.show(10)
        //println("Time : " + time + "rdd - id" + rdd.id)
        //TODO : Save To Postgres.

        try{
          PostgresIO.writeToPostgres(ss, result_inf_tmp, bJdbcURL.value, "result_inf_tmp", SaveMode.Overwrite, bPgProperties.value)
        }catch{
          case e: SQLException => System.err.println("SQLException occur when save result_inf_tmp" + e.getSQLState + " " + e.getMessage)
          case e: Exception => System.err.println("UncatchException occur when save result_inf_tmp: " +  e.getMessage)
          case _ => println("Ignore !")
        }
        df.unpersist()
        val host_endpoint_id_df = result_inf_tmp.select("host_endpoint")
        //host_endpoint_id_df.show()
        val host_endpoint_ids: Array[Any] = host_endpoint_id_df.rdd.map(r => r(0)).collect()

        if (host_endpoint_ids.length > 0) {
          var host_endpoint_IdsString = "("
          host_endpoint_ids.foreach { x =>
            val y = "'" + x + "',"
            host_endpoint_IdsString = host_endpoint_IdsString + y
          }
          host_endpoint_IdsString = host_endpoint_IdsString.dropRight(1) + ")"


          val insertINFIndexQuery = "insert into dwh_inf_index(bras_id,host_endpoint,host,module_ol,index,cpe_error,lostip_error,date_time)" +
            " select bh.bras_id,i.host_endpoint,i.host,i.module_ol,i.index_ol,i.cpe_error,i.lostip_error,i.date_time from result_inf_tmp i join " +
            "(SELECT * FROM brashostmapping WHERE host_endpoint in "+host_endpoint_IdsString+ " ) bh on i.host_endpoint = bh.host_endpoint "

          //println(insertINFIndexQuery)
          PostgresIO.pushDownJDBCQuery(insertINFIndexQuery,bJdbcURL.value)
          //

          val insertINFModuleQuery = "insert into dwh_inf_module(bras_id,host,module,cpe_error,lostip_error,date_time) " +
            "select bh.bras_id,i.host,i.module_ol,SUM(i.cpe_error), SUM(i.lostip_error),i.date_time " +
            "from result_inf_tmp i join (SELECT * FROM brashostmapping WHERE host_endpoint in " + host_endpoint_IdsString +
            ") bh on i.host_endpoint = bh.host_endpoint GROUP BY i.host,bh.bras_id,i.date_time,i.module_ol ;"
          //println(insertINFModuleQuery)

          val insertINFHostQuery = "insert into dwh_inf_host(bras_id,host,cpe_error,lostip_error,date_time) " +
            "select bh.bras_id,i.host,SUM(i.cpe_error),SUM(i.lostip_error),i.date_time " +
            "from result_inf_tmp i join (SELECT * FROM brashostmapping WHERE host_endpoint in " + host_endpoint_IdsString +
            ") bh on i.host_endpoint = bh.host_endpoint GROUP BY i.host,bh.bras_id,i.date_time ;"
          //println(insertINFHostQuery)



          // CO VE KHONG OK LAM, JOB DIE WITHOUT ERROR CODE AFTER 14 DAYS.
          // Back to normal.

          /*// Set number of threads via a configuration property
          val pool = Executors.newFixedThreadPool(3)
          // create the implicit ExecutionContext based on our thread pool
          implicit val xc = ExecutionContext.fromExecutorService(pool)
          // create two async task.
          try{
            val insertINFHostTask = PostgresIO.pushDownQueryAsync(insertINFHostQuery,bJdbcURL.value)
            val insertINFModuleTask = PostgresIO.pushDownQueryAsync(insertINFModuleQuery,bJdbcURL.value)
            Await.result(Future.sequence(Seq(insertINFHostTask,insertINFModuleTask)), scala.concurrent.duration.Duration(5,SECONDS))
          }catch{
            case e: SQLException => System.err.println("SQLException occur when save host-module : " + e.getSQLState + " " + e.getMessage)
            case e: Exception => System.err.println("UncatchException occur when save host-module : " +  e.getMessage)
            case _ => println("Ignore !")
          }*/

          try{
            PostgresIO.pushDownJDBCQuery(insertINFHostQuery,bJdbcURL.value)
            PostgresIO.pushDownJDBCQuery(insertINFModuleQuery,bJdbcURL.value)

          }catch{
            case e: SQLException => System.err.println("SQLException occur when save host-module : " + e.getSQLState + " " + e.getMessage)
            case e: Exception => System.err.println("UncatchException occur when save host-module : " +  e.getMessage)
            case _ => println("Ignore !")
          }


          //Await.result(Future.sequence(Seq(taskA,taskB)), Duration(1, MINUTES))
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
  def pushDownQueryAsync(query: String,url: String)(implicit xc: ExecutionContext) = Future {
    PostgresIO.pushDownJDBCQuery(query,url)
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
  //.filter(x => x.hostName != "n/a")


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


  def insertIteratorToPostgres(iter: Iterator[ModermEvent2],jdbcURL: String): Unit= {
    val insertQuer: String = " insert into devices_history( index_ol, log_type, module_ol, times_stamp ) values (?,?,?,?) ON CONFLICT DO NOTHING"

    var conn: Connection = null

    try{
      Class.forName("org.postgresql.Driver").newInstance
      conn = DriverManager.getConnection(jdbcURL)
      val prepareStm =  conn.prepareStatement(insertQuer)
      iter.foreach{item =>
        prepareStm.setString(1,item.hostName)
        prepareStm.setString(2,item.error)
        prepareStm.setString(3,item.olt)
        prepareStm.setTimestamp(4,item.time)
        prepareStm.execute()
      }
    }catch {
      case ex: SQLException => System.err.println(ex.getMessage)
    }finally {
      conn.close()
    }
  }


}





case class HostErrorCountObject(hostName: String, error: String, time: Timestamp, count: Int) extends Serializable {}
case class ModermEvent(hostName: String, error: String, olt: String, time: Long) extends  Serializable{}
case class ModermEvent2(hostName: String, error: String, olt: String, time: Timestamp) extends  Serializable{}