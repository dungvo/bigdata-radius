package streaming_jobs.conn_jobs

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, Properties, UUID}

import org.apache.log4j.Logger
import com.datastax.spark.connector.SomeColumns
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{Accumulable, SparkContext, TaskContext}
import org.apache.spark.sql._
import parser.{AbtractLogLine, ConnLogLineObject, ConnLogParser}
import com.datastax.spark.connector.streaming._
import core.streaming.SparkSessionSingleton
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.sql._
import org.apache.spark.sql.functions._

import scalaj.http.{Http, HttpOptions}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import core.KafkaProducerFactory
import core.sinks.KafkaDStreamSinkExceptionHandler
import org.apache.spark.sql.expressions.Window

//import scala.collection.Map
import core.streaming.{DurationBoadcast, ParserBoacast}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import storage.postgres.PostgresIO
import org.elasticsearch.spark._

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex
import streaming_jobs.conn_jobs.ConcurrentHashMapAccumulator

/**
  * Created by hungdv on 20/03/2017.
  */
/*
TODO :  Using  strategy   pattern,  with abtract StorageWriter,
// PostgresIO, ESIO, MongoIO all are inheritaged storagewriter and have method persistToStorage.
 */
object ParseAndCountConnLog {
  val logger = Logger.getLogger(getClass)
  type WordCount = (String,Int)
  def today = ("radius-" + org.joda.time.DateTime.now().toString("yyyy-MM-dd"))
  def parseAndCount(ssc: StreamingContext,
                    ss:SparkSession,
                    lines: DStream[String],
                    windowDuration: FiniteDuration,
                    slideDuration: FiniteDuration,
                    cassandraConfig: Map[String,Object],
                    mongoDbConfig: Map[String,Object],
                    conLogParser: ConnLogParser,
                    lookupCache: Broadcast[collection.Map[String,String]],
                    postgresConfig: Predef.Map[String,String],
                    powerBIConfig: Map[String,String],
                    radiusAnomalyDetectionKafkaTopic: String,
                    producerConfig: Predef.Map[String,String]
                   ): Unit ={

    import scala.language.implicitConversions
    implicit def finiteDurationToSparkDuration(value: FiniteDuration): org.apache.spark.streaming.Duration =
      new Duration(value.toMillis)
    import ss.implicits._

    val sc = ssc.sparkContext
    //val mc = new MapAccumulator()

    val bWindowDuration = DurationBoadcast.getInstance(sc,windowDuration)
    val bSlideDuration  = DurationBoadcast.getInstance(sc,slideDuration)
    val bConLogParser   = ParserBoacast.getInstance(sc,conLogParser)
    val bConnectType    = sc.broadcast(Seq("SignIn","LogOff"))
    val bUrlBrasCount: Broadcast[String] = sc.broadcast(powerBIConfig("bras_dataset_url"))
    val bUrlInfCount: Broadcast[String] = sc.broadcast(powerBIConfig("inf_dataset_url"))
    val bUrlBrasSumCount: Broadcast[String] = sc.broadcast(powerBIConfig("bras_sumcount_dataset_url"))
    val bProducerConfig: Broadcast[Map[String, String]] = sc.broadcast(producerConfig)
    //val bProducerConfig: Broadcast[Predef.Map[String,Object]] = sc.broadcast(producerConfig)
    val bAnomalyDetectKafkaTopic: Broadcast[String] = sc.broadcast(radiusAnomalyDetectionKafkaTopic)
    //val bGson = sc.broadcast[Gson](new Gson())
    // PG properties :
    val jdbcUrl = PostgresIO.getJDBCUrl(postgresConfig)
    val bJdbcURL = sc.broadcast(jdbcUrl)
    //FIXME :
    // Ad-hoc fixing
    val pgProperties    = new Properties()
    pgProperties.setProperty("driver","org.postgresql.Driver")
    val bPgProperties   = sc.broadcast(pgProperties)
 /*   val bWindowDuration = sc.broadcast(windowDuration)
    val bSlideDuration  = sc.broadcast(slideDuration)
    val bConLogParser   = sc.broadcast(conLogParser)*/

    val lookupHostName: (String => String) = (arg: String) =>{
      lookupCache.value.getOrElse(arg,"N/A")
    }
    val sqlLookup = org.apache.spark.sql.functions.udf(lookupHostName)

    val objectConnLogs: DStream[ConnLogLineObject] = lines.transform(extractValue(bConLogParser))

    // SAVE TO CASSANDRA
/*        objectConnLogs.saveToCassandra(cassandraConfig("keySpace").toString,
                                       cassandraConfig("table").toString,
                                       SomeColumns("time","session_id","connect_type","name","content1","content2"))*/
    // Save to ES :
    import storage.es.ElasticSearchDStreamWriter._
    //var today = org.joda.time.DateTime.now().toString("yyyy-MM-dd")

    objectConnLogs.persistToStorageDaily(Predef.Map[String,String]("indexPrefix" -> "radius-connlog","type" -> "connlog"))
    //objectConnLogs.persistToStorage(Predef.Map[String,String]("index" -> ("radius-" + today),"type" -> "connlog"))
    //objectConnLogs.persistToStorage(Predef.Map[String,String]("index" -> ("radius-test-" + today),"type" -> "connlog"))

    //Sorry, it was 7PM, i was too lazy to code. so i did too much hard code here :)).
    val connType = objectConnLogs
      .map(conlog => (conlog.connect_type,1))
      .reduceByKeyAndWindow( _ + _ , _ -_ , bWindowDuration.value,bSlideDuration.value)
      .transform(skipEmptyWordCount)  //Uncomment this line to remove empty wordcoutn such as : SignInt : Count 0

    /*/*
    Accumulative count
    // This shit is so cool, but we don't need it anymore :)).
    */
    // val accumutiveCounting = new ConcurrentHashMapAccumulator()
    // FIXME : Using Singleton object to register Accumulator!!
    // sc.register(accumutiveCounting)
    // Get accumulator instance from SingletonObject.
    val accumutiveCounting = SingletonAccumulator.getInstance(sc)
      connType.foreachRDD { rdd =>
        //val spark = SparkSession.builder().getOrCreate()
        //val acc = mc.getInstance(sc)
        //val acc = mc.getInstance(spark.sparkContext)
        if(!rdd.isEmpty()) {
          val context = rdd.sparkContext
          // val acc = mc.getInstance(context) //////// Raise of null point exception.
          rdd.foreach{case(word,count) => accumutiveCounting add(word -> count)}
          val accValue: mutable.Map[String, Int] = accumutiveCounting.value
          // Convert Map to Rdd
          val accRDD: RDD[StatusCount] = context.parallelize(accValue.toSeq)
                                                .map{case(typeName,count)=> StatusCount(typeName,count)}
          //val accDF                    = accRDD.toDF()
          //println(accValue)
          // FIXME need to consider SaveMode.
          // Xem lai cho nay can append hay override
          //accDF.write.mode(SaveMode.Append)
          //SAVE To MONGO
          /*accRDD.toDF().write.mode(SaveMode.Append)
               .mongo(WriteConfig(Map("collection"->"connlog_accumulative_count"),Some(WriteConfig(context))))*/
          // Old API
          //MongoSpark.save(accDF.write.option("collection", "connlog_accumulative_count").mode("overwrite"))
          //Save Accumulative to Postgres
          val accDF = accRDD.toDF("typename","count").withColumn("time",org.apache.spark.sql.functions.current_timestamp())
          if(accDF.count() > 0) {

            //Save to Postgres
            PostgresIO.writeToPostgres(ss, accDF, bJdbcURL.value, "acc_counting", SaveMode.Append, bPgProperties.value)
          }

        }
      }*/

    connType.transform(toWouldCountObject)
      .foreachRDD{rdd =>

        //Save Conn Counting to Es
        //var indexName = "radius-" + today
        //val indexName = "radius-test-" + today
        var typeName  = "conn_counting"
        rdd.saveToEs("radius-" + org.joda.time.DateTime.now().toString("yyyy-MM-dd") + "/" + typeName)
        //rdd.saveToEs(indexName + "/" + typeName)

        // Write config should put in SparkConfig.
        //This still a reliable way to write from a stream to mongo.
        //data.write.mode(SaveMode.Append).mongo()
        //FIXME What the fuck is thit shit ??!!

        //SAVE CONN_COUNTING to  mongo
        //rdd.toDF().write.mode(SaveMode.Append).mongo()

        //SAVE CONN_COUNTING to postgres
        // Ver 1.0
        //val data = rdd.toDF("typename","count","time")
        // Pre ver 1.0
        //val data = rdd.toDF("typename","count").withColumn("time",org.apache.spark.sql.functions.current_timestamp())
        //Save Conn Counting to Postgres
        //PostgresIO.writeToPostgres(ss,data,bJdbcURL.value,"conn_counting",SaveMode.Append,bPgProperties.value)

        //data.ma
        //Mongo Spark Connector 2.0 API :
        //@since 2017-03-22
        //MongoSpark.save(data.write.option("collection","collectionName").mode("overwrite"))
    }

    objectConnLogs.foreachRDD({
      (rdd: RDD[ConnLogLineObject],time_ : Time) =>
        // Get sparkContext from rdd
        val context = rdd.sparkContext
        // Get the singleton instance of SparkSession
        //val sparkSession = SparkSessionSingleton.getInstance(context.getConf)
        //val sparkSession = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        //import sparkSession.implicits._
        // Convert RDD[ConnLogObject] to RDD
        // Signin - logoff logs have bras name in content 1 -> filter out reject.
        /////////////////////////////////////// BRAS ////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////
        val brasInfo = rdd.filter(line => line.connect_type != ConnectTypeEnum.Reject.toString)
                            .toDF("time","session_id","connect_type","name","content1","content2")
                            .select("name","connect_type","content1")
       /* val timeFunc: (AnyRef => String) = (arg: AnyRef) => {
          getCurrentTime()
        }
        val sqlTimeFunc = udf(timeFunc)*/
        val brasCount = brasInfo.groupBy(col("content1"),col("connect_type"))
          .agg(count(col("name")).as("count_by_bras"),countDistinct(col("name")).as("count_distinct_by_bras"))

         val brasCountTotalPivot =  brasCount.groupBy("content1").pivot("connect_type",bConnectType.value)
                                                .agg(expr("coalesce(first(count_by_bras),0)"))
                                                 .withColumnRenamed("SignIn","signin_total_count")
                                                 .withColumnRenamed("LogOff","logoff_total_count")

        val brasCountDistinctPivot = brasCount.groupBy("content1").pivot("connect_type",bConnectType.value)
                                                .agg(expr("coalesce(first(count_distinct_by_bras),0)"))
                                                .withColumnRenamed("SignIn","signin_distinct_count")
                                                .withColumnRenamed("LogOff","logoff_distinct_count")

        val brasCountPivot: DataFrame = brasCountTotalPivot.join(brasCountDistinctPivot,"content1")
                                               //.withColumn("time",sqlTimeFunc(col("content1")))
                                               .withColumn("time",org.apache.spark.sql.functions.current_timestamp())
                                               .withColumnRenamed("content1","bras_id")
                                               //.cache()

        //println(s"========= $time_ =========")
        if(brasCountPivot.count() > 0) {
          //brasCount.show()
          // Save aggregated results to MONGO
          /*brasCount.write.mode(SaveMode.Append)
                    .mongo(WriteConfig(Map("collection"->"connlog_bras_count"),Some(WriteConfig(context))))*/

          val brasSumCounting = brasCountPivot.agg(sum(col("signin_total_count")).as("total_signin"),
            sum(col("logoff_total_count")).as("total_logoff"),
            sum(col("signin_distinct_count")).as("total_signin_distinct"),
            sum(col("logoff_distinct_count")).as("total_logoff_distinct"))
            .withColumn("time",org.apache.spark.sql.functions.current_timestamp())


          val head: Row = brasSumCounting.head(1).toList(0)

          val signInTotalCount = head.getAs[Long]("total_signin")
          val logOffTotalCount = head.getAs[Long]("total_logoff")
          val signInDistinctTotalCount = head.getAs[Long]("total_signin_distinct")
          val logOffDistinctTotalCount = head.getAs[Long]("total_logoff_distinct")
          val timeBrasCount = head.getAs[java.sql.Timestamp]("time")

          // Consume to many time.
//          val signInTotalCount = brasSumCounting.head(1).map(row => row.getAs[Long]("total_signin")).toList(0)
//          val logOffTotalCount = brasSumCounting.head(1).map(row => row.getAs[Long]("total_logoff")).toList(0)
//          val signInDistinctTotalCount = brasSumCounting.head(1).map(row => row.getAs[Long]("total_signin_distinct")).toList(0)
//          val logOffDistinctTotalCount = brasSumCounting.head(1).map(row => row.getAs[Long]("total_logoff_distinct")).toList(0)
//          val timeBrasCount = brasSumCounting.head(1).map(row => row.getAs[java.sql.Timestamp]("time")).toList(0)



/*          var jsonString =
            s"""
               [
               {
               "signin_total" :${signInTotalCount},
               "logoff_total" :${logOffTotalCount},
               "signin_user" :${signInDistinctTotalCount},
               "logoff_user" :${logOffDistinctTotalCount},
               "time" :${timeBrasCount.toString}
               }
               ]
           """*/

          val brasSumCountTotal = new BrasSumCount(signin_total= signInTotalCount,
            logOffTotalCount,
            signin_user = signInDistinctTotalCount,
            logoff_user = logOffDistinctTotalCount,
            time = timeBrasCount)
          val arrayListType = new TypeToken[java.util.ArrayList[BrasSumCount]]() {}.getType
          val brasSumCountMetrics = new util.ArrayList[BrasSumCount]()
          brasSumCountMetrics.add(brasSumCountTotal)
          val  brasSumCountJson =    new Gson().toJson(brasSumCountMetrics,arrayListType)
          val httpBrasSumcount = Http(bUrlBrasSumCount.value).proxy(powerBIConfig("proxy_host"),80)
          try {
            val resultBrasSumcount = httpBrasSumcount.postData(brasSumCountJson)
              .header("Content-Type", "application/json")
              .header("Charset", "UTF-8")
              .option(HttpOptions.readTimeout(15000)).asString
            logger.info(s"Send BRAS SUM COUNT metrics to PowerBi - Statuscode : ${resultBrasSumcount.statusLine}.")
          }  catch {
            case e:java.net.SocketTimeoutException => logger.error(s"Time out Exception when sending BRAS counting result to BI")
            case _: Throwable => println("Just ignore this shit.")
          }


          //SEND TO PowerBI:
          //val window3 = Window.partitionBy("bras_id").orderBy($"sum".desc)
          val sumUp = brasCountPivot.withColumn("sum",$"signin_total_count" + $"logoff_total_count")

                                            //.withColumn("rank_sum",rank().over(window3))
            val top50Sum = sumUp.select(col("bras_id"),col("signin_total_count"),col("logoff_total_count"),col("signin_distinct_count"),col("logoff_distinct_count"),col("time"))
                                .orderBy(col("sum").desc)
                                .limit(50)
            //.where(col("rank_sum") <= lit(50))

          val brasCountObjectRDD = top50Sum.rdd.map{ row =>
          //val brasCountObjectRDD = brasCountPivot.rdd.map{ row =>
            val brasCount: BrasCountObject = new BrasCountObject(
              row.getAs[String]("bras_id"),
              row.getAs[Long]("signin_total_count").toInt,
              row.getAs[Long]("logoff_total_count").toInt,
              row.getAs[Long]("signin_distinct_count").toInt,
              row.getAs[Long]("logoff_distinct_count").toInt,
              row.getAs[java.sql.Timestamp]("time")
            )
            brasCount
          }
          var brasCountIndex = "count_by_bras-"+org.joda.time.DateTime.now().toString("yyyy-MM-dd") +"-" + "%02d".format(Calendar.getInstance().get(Calendar.HOUR_OF_DAY))
          var brasCountType = "bras_count"

          // Make rdd from sequence then save to postgres
          //SAVE TO ES
          brasCountObjectRDD.saveToEs("count_by_bras-"+org.joda.time.DateTime.now().toString("yyyy-MM-dd") +"-" + "%02d".format(Calendar.getInstance().get(Calendar.HOUR_OF_DAY)) + "/" + brasCountType)
          // Send to PowerBI and KAFKA
          brasCountObjectRDD.foreachPartition{partition =>
            val copy = partition

            // Maybe we have many null partitions.
            if(partition.hasNext){
              val arrayListType = new TypeToken[java.util.ArrayList[BrasCountObject]]() {}.getType
              val gson = new Gson()
              val metrics = new util.ArrayList[BrasCountObject]()
              partition.foreach(bras => metrics.add(bras))
              val metricsJson = gson.toJson(metrics, arrayListType)
              val http = Http(bUrlBrasCount.value).proxy(powerBIConfig("proxy_host"),80)
              try{
                val result = http.postData(metricsJson)
                  .header("Content-Type", "application/json")
                  .header("Charset", "UTF-8")
                  .option(HttpOptions.readTimeout(15000)).asString
                logger.info(s"Send BRAS metrics to PowerBi - Statuscode : ${result.statusLine}.")
              }catch
                {
                  case e:java.net.SocketTimeoutException => logger.error(s"Time out Exception when sending INF counting result to BI")
                  case _: Throwable => println("Just ignore this shit.")
                }

              // Named the index
              //val brasCountIndex = "count_by_bras-"+org.joda.time.DateTime.now().toString("yyyy-MM-dd")+"-"  + Calendar.getInstance().get(Calendar.HOUR_OF_DAY)
              //val brasCountType = "bras_count"

              // Make rdd from sequence then save to postgres
              //SAVE TO ES

              //brasCountRDD.saveToEs(brasCountIndex + "/" + brasCountType)
              //context.makeRDD(metrics.toArray()).saveToEs(brasCountIndex + "/" + brasCountType)
              //sc.makeRDD(metrics.toArray()).saveToEs(brasCountIndex + "/" + brasCountType)

              //println(result.statusLine)
              // S

            }
          }
          // We need to iterator twitte
          brasCountObjectRDD.foreachPartition{partition =>
            if(partition.hasNext){
              val producer: KafkaProducer[String,String] = KafkaProducerFactory.getOrCreateProducer(bProducerConfig.value)
              val context = TaskContext.get()
              val callback = new KafkaDStreamSinkExceptionHandler
              //val logger = Logger.getLogger(getClass)
              //logger.debug(s"Send Spark partition: ${context.partitionId()} to Kafka topic in [anomaly]")
              partition.map{countObjectRDD =>
                // BRASID:LOGOFFTOTAL:SIGNINTOTAL:LOGOFFDISTINCT:SIGNINDISTINCT:TIME
                val string = countObjectRDD.bras_id+"#"+countObjectRDD.signin_total_count+"#"+countObjectRDD.logoff_total_count+"#"+countObjectRDD.signin_distinct_count+"#"+countObjectRDD.logoff_distinct_count+"#"+countObjectRDD.time
                //val record = new ProducerRecord[String,String](bAnomalyDetectKafkaTopic.value,"anomaly",string)
                //Hope this will help.

                val record = new ProducerRecord[String,String](bAnomalyDetectKafkaTopic.value,UUID.randomUUID().toString,string)
                callback.throwExceptionIfAny()
                producer.send(record,callback)
              }.toList
            }
          }
          //Send To Kafka - For Anomaly detection

          //brasCountPivot.unpersist()
          // old version - after 1.0
/*          val metrics = new util.ArrayList[BrasCountObject]()
          brasCountPivot.rdd.foreachPartition { partition =>
            if(partition.hasNext) {
              val arrayListType = new TypeToken[java.util.ArrayList[BrasCountObject]]() {}.getType
              val gson = new Gson()

              partition.foreach{ row =>
                val brasCount: BrasCountObject = new BrasCountObject(
                  row.getAs[String]("bras_id"),
                  row.getAs[Long]("signin_total_count").toInt,
                  row.getAs[Long]("logoff_total_count").toInt,
                  row.getAs[Long]("signin_distinct_count").toInt,
                  row.getAs[Long]("logoff_distinct_count").toInt,
                  row.getAs[java.sql.Timestamp]("time")
                )
                metrics.add(brasCount)
                //brasCount
              }
              //brasCountRDD.foreach{bras => metrics.add(bras)}

              val metricsJson = gson.toJson(metrics, arrayListType)
              val http = Http(bUrlBrasCount.value).proxy(powerBIConfig("proxy_host"),80)
              val result = http.postData(metricsJson)
              .header("Content-Type", "application/json")
              .header("Charset", "UTF-8")
              .option(HttpOptions.readTimeout(10000)).asString
            logger.info(s"Send BRAS metrics to PowerBi - Statuscode : ${result.statusLine}.")
              // Named the index
            val brasCountIndex = "count_by_bras-"+today  + Calendar.getInstance().get(Calendar.HOUR_OF_DAY)
            val brasCountType = "bras_count"

             // Make rdd from sequence then save to postgres
              //SAVE TO ES

              //brasCountRDD.saveToEs(brasCountIndex + "/" + brasCountType)
              //context.makeRDD(metrics.toArray()).saveToEs(brasCountIndex + "/" + brasCountType)
              //sc.makeRDD(metrics.toArray()).saveToEs(brasCountIndex + "/" + brasCountType)

            //println(result.statusLine)
            }
          }*/

          //VER 1.0
          // Save aggregated results to Postgres
          // PostgresIO.writeToPostgres(ss,brasCountPivot,bJdbcURL.value,"bras_count",SaveMode.Append,bPgProperties.value)
          // Save aggregated results to ES :

        }
        /////////////////////////////////////// INF ////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        val infInfo   =  rdd.toDF("time","session_id","connect_type","name","content1","content2")
                            .select("name","connect_type")
        val infMerged =  infInfo.withColumn("host",sqlLookup(col("name")))
        val infCount  =  infMerged.groupBy(col("host"),col("connect_type"))
                    .agg(count(col("name")).as("count_by_inf"),countDistinct(col("name")).as("count_distinct_by_inf"))
                    //.withColumn("time",sqlTimeFunc(col("host")))
        val infCountTotalPivot = infCount.groupBy("host").pivot("connect_type",bConnectType.value)
                                            .agg(expr("coalesce(first(count_by_inf),0)"))
                                            .withColumnRenamed("SignIn","signin_total_count")
                                            .withColumnRenamed("LogOff","logoff_total_count")
        val infCountDistinctPivot = infCount.groupBy("host").pivot("connect_type",bConnectType.value)
                                            .agg(expr("coalesce(first(count_distinct_by_inf),0)"))
                                            .withColumnRenamed("SignIn","signin_distinct_count")
                                            .withColumnRenamed("LogOff","logoff_distinct_count")

        val infCountPivot = infCountTotalPivot.join(infCountDistinctPivot,"host")
          //.withColumn("time",sqlTimeFunc(col("host")))
          .withColumn("time",org.apache.spark.sql.functions.current_timestamp())
          //.withColumn("time",org.apache.spark.sql.functions.current_timestamp())
          //.withColumnRenamed("content1","bras_id")

        //println(s"========= $time_ =========")
        if(infCountPivot.count() > 0) {
          //Some day, may be we will need sum all signin logoff by inf.

          //Convert DF to RDD[InfCountObject]
          val infCountObjectRDD = infCountPivot.rdd.map{ row =>
            val infCount = new InfCountObject(
              row.getAs[String]("host"),
              row.getAs[Long]("signin_total_count").toInt,
              row.getAs[Long]("logoff_total_count").toInt,
              row.getAs[Long]("signin_distinct_count").toInt,
              row.getAs[Long]("logoff_distinct_count").toInt,
              row.getAs[Timestamp]("time")
            )
            infCount
          }
          //Save to es - named index and type
          val infCountIndex = "count_by_inf-"+org.joda.time.DateTime.now().toString("yyyy-MM-dd") +"-" + "%02d".format(Calendar.getInstance().get(Calendar.HOUR_OF_DAY))
          val infCountType = "inf_count"
          // Make rdd from sequence then save to postgres
          //SAVE TO ES
          infCountObjectRDD.saveToEs("count_by_inf-"+org.joda.time.DateTime.now().toString("yyyy-MM-dd") +"-" + "%02d".format(Calendar.getInstance().get(Calendar.HOUR_OF_DAY)) + "/" + infCountType)
          // Traveser rdd.foreachPartition -> add element to metrics Array
          //TODO Uncomment this block to ENABLE sending inf metric to power BI
          //TODO : Begin
        /*  infCountObjectRDD.foreachPartition{partition =>
            if(partition.hasNext) {
              val infMetrics = new util.ArrayList[InfCountObject]()
              val arrayListType = new TypeToken[java.util.ArrayList[InfCountObject]]() {}.getType
              val gson = new Gson()
              partition.foreach { infCount =>
                infMetrics.add(infCount)
              }
              val infMetricsJson = gson.toJson(infMetrics, arrayListType)
              val infHttp = Http(bUrlInfCount.value).proxy(powerBIConfig("proxy_host"),80)
              val infResult = infHttp.postData(infMetricsJson)
                .header("Content-Type", "application/json")
                .header("Charset", "UTF-8")
                .option(HttpOptions.readTimeout(10000)).asString
              logger.info(s"Send INF metrics to PowerBi - Statuscode : ${infResult.statusLine}.")
            }
          }*/
          //TODO : End
          // OLD VERSION 1.0
          /*val infMetrics = new util.ArrayList[InfCountObject]()
          infCountPivot.rdd.foreachPartition { partition =>
            if(partition.hasNext) {
              val arrayListType = new TypeToken[java.util.ArrayList[InfCountObject]]() {}.getType
              val gson = new Gson()
              partition.foreach { row =>
                val infCount = new InfCountObject(
                  row.getAs[String]("host"),
                  row.getAs[Long]("signin_total_count").toInt,
                  row.getAs[Long]("logoff_total_count").toInt,
                  row.getAs[Long]("signin_distinct_count").toInt,
                  row.getAs[Long]("logoff_distinct_count").toInt,
                  row.getAs[Timestamp]("time")
                )
                infMetrics.add(infCount)
              }
              val infMetricsJson = gson.toJson(infMetrics, arrayListType)
              val infHttp = Http(bUrlBrasCount.value).proxy(powerBIConfig("proxy_host"),80)
              val infResult = infHttp.postData(infMetricsJson)
                .header("Content-Type", "application/json")
                .header("Charset", "UTF-8")
                .option(HttpOptions.readTimeout(10000)).asString
              logger.info(s"Send INF metrics to PowerBi - Statuscode : ${infResult.statusLine}.")

              // Named the index
              //val brasCountIndex = "count_by_inf-" +today+ Calendar.getInstance().get(Calendar.HOUR_OF_DAY)
              //sval brasCountType = "inf_count"

              // Make rdd from sequence then save to postgres
              //SAVE TO ES
              //context.makeRDD(infMetrics.toArray()).saveToEs(brasCountIndex + "/" + brasCountType)
              //sc.makeRDD(infMetrics.toArray()).saveToEs(brasCountIndex + "/" + brasCountType)

              //println(result.statusLine)
            }

          // SAVE Aggregated log to MONGO.
          /*infCount.write.mode(SaveMode.Append)
            .mongo(WriteConfig(Map("collection"->"connlog_inf_count"),Some(WriteConfig(context))))*/
          // SAVE Aggreagated result to Postgres
          //PostgresIO.writeToPostgres(ss,infCountPivot,bJdbcURL.value,"inf_count",SaveMode.Append,bPgProperties.value)
        }*/

      }
    })
    
      //rdd.map()
  }


  def toLowerCase   = (lines: RDD[String]) => lines.map(words => words.toLowerCase)
  //FIXME !!!
  def extractValue  = (parser: Broadcast[ConnLogParser]) => (lines: RDD[String]) =>
    lines.map{ line =>
      val parsedObject = parser.value.extractValues(line).getOrElse(None)
      parsedObject match{
        case Some(x) => x.asInstanceOf[ConnLogLineObject]
        case _ => None
      }
      parsedObject
    }.filter(x => x != None).map(ob => ob.asInstanceOf[ConnLogLineObject])

  /*def extractValue  = (parser: Broadcast[ConnLogParser]) => (lines: RDD[String]) =>
  lines.map(line => parser.value.extractValues(line).get.asInstanceOf[ConnLogLineObject])*/
  def toWouldCountObject = (streams : RDD[(String,Int)]) =>
    streams.map(tuple => StatusCount(tuple._1.toString, tuple._2, new Timestamp(System.currentTimeMillis())))
  //def toWouldCountObject = (streams : RDD[(String,Int)]) => streams.map(tuple => StatusCount(tuple._1.toString, tuple._2))
  def getCurrentTime():String ={
    val now: Date = Calendar.getInstance().getTime
    val nowFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val nowFormeted: String = nowFormater.format(now)
    //val nowFormeted = nowFormater.format(now).toString
    nowFormeted
  }
  def skipEmptyWordCount = (streams : RDD[(String,Int)]) => streams.filter(wordCount => wordCount._2 > 0)
  }

case class StatusCount(connType: String,count: Int, time: Timestamp) extends Serializable{}
case class BrasSumCount(signin_total: Long,logoff_total: Long,signin_user:Long,logoff_user: Long, time: Timestamp) extends Serializable{}
//case class StatusCount(connType: String,count: Int) extends Serializable{}
//case class StatusCount(connType: String,count: Int,time: String) extends Serializable{}

// case class BRASCountSchema(name: String, brasName: String, type_ : String) extends Serializable{}
// Connect type : Reject - sign in - log off.
object ConnectTypeEnum extends Enumeration{
  type ConnectType = Value
  val Reject = Value("Reject")
  val SignIn = Value("SignIn")
  val LogOff  = Value("LogOff")
}
/*
object dateTimeTest{
  def main(args: Array[String]): Unit = {
    val now = Calendar.getInstance()
    println(getCurrentTime)
  }
  def getCurrentTime():String ={
    val now = Calendar.getInstance().getTime
    val nowFormater = new SimpleDateFormat("yyyy/MM/dd - hh:mm:ss")
    val nowFormeted = nowFormater.format(now).toString
    return nowFormeted
  }
}
*/

