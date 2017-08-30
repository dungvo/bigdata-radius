package streaming_jobs.conn_jobs

import java.sql._
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, Properties, UUID}

import org.apache.spark.sql.cassandra._
import org.apache.log4j.{Level, Logger}
import com.datastax.spark.connector.SomeColumns
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{Accumulable, SparkConf, SparkContext, TaskContext}
import org.apache.spark.sql._
import parser.{AbtractLogLine, ConnLogLineObject, ConnLogParser}
import com.datastax.spark.connector.streaming._
import core.streaming.{RedisClientFactory, SparkSessionSingleton}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.sql._
import org.apache.spark.sql.functions._

import scalaj.http.{Http, HttpOptions}
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.redis.RedisClientPool
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
                    //lookupCache: Broadcast[collection.Map[String,String]],
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
    //val bUrlInfCount: Broadcast[String] = sc.broadcast(powerBIConfig("inf_dataset_url"))
    val bUrlBrasSumCount: Broadcast[String] = sc.broadcast(powerBIConfig("bras_sumcount_dataset_url"))
    val bProducerConfig: Broadcast[Map[String, String]] = sc.broadcast(producerConfig)
    //val bProducerConfig: Broadcast[Predef.Map[String,Object]] = sc.broadcast(producerConfig)
    val bAnomalyDetectKafkaTopic: Broadcast[String] = sc.broadcast(radiusAnomalyDetectionKafkaTopic)
    val bBrasNameLookUp: Broadcast[Predef.Map[String,String]] = sc.broadcast(Predef.Map("HCM-MP03-2-NEW" -> "HCM-MP-03-02",
      "VPC-MP01" -> "VPC-MP-01-01", "HN-MP02-8" -> "HNI-MP-02-08", "LGI-MP01-2" -> "LGI-MP-01-02", "DTP-MP01-2" -> "DTP-MP-01-02",
      "TQG-MP01-1" -> "TQG-MP-01-01", "HDG-MP01-3" -> "HDG-MP-01-03", "VLG-MP01-1" -> "VLG-MP-01-01", "AGG-MP01-1" -> "AGG-MP-01-01",
      "HN-MP01-1" -> "HNI-MP-01-01", "HN-MP05-1" -> "HNI-MP-05-01", "DNI-MP01-2-NEW" -> "DNI-MP-01-02", "VTU-MP01-1-NEW" -> "VTU-MP-01-01",
      "DLK-MP01-2" -> "DLK-MP-01-02", "BDH-MP01-2" -> "BDH-MP-01-02", "QBH-MP01-2" -> "QBH-MP-01-02", "TNH-MP01-1" -> "TNH-MP-01-01",
      "HN-MP03-1" -> "HNI-MP-03-01", "CMU-MP01-2" -> "CMU-MP-01-02", "BNH-MP01-3" -> "BNH-MP-01-03", "THA-MP01-2" -> "THA-MP-01-02",
      "NAN-MP01-3" -> "NAN-MP-01-03", "PYN-MP01-2" -> "PYN-MP-01-02", "HN-MP02-5" -> "HNI-MP-02-05", "TNN-MP03" -> "TNN-MP-01-03",
      "TGG-MP01-2" -> "TGG-MP-01-02", "IXIA-test" -> "QNH-MP-01-02", "KGG-MP01-3" -> "KGG-MP01-3", "LDG-MP01-2" -> "LDG-MP-01-02",
      "QNI-MP01-1" -> "QNI-MP-01-01", "QNH-MP03" -> "QNH-MP-01-03", "YBI-MP02" -> "YBI-MP-01-02", "DLK-MP01-4" -> "DLK-MP-01-04",
      "QNM-MP01-2" -> "QNM-MP-01-02", "BPC-MP01-1" -> "BPC-MP-01-01", "GLI-MP01-2" -> "GLI-MP-01-02", "VTU-MP01-2-NEW" -> "VTU-MP-01-02",
      "LSN-MP02" -> "LSN-MP-01-02", "HCM-MP06-1" -> "HCM-MP-06-01", "HN-MP02-1-NEW" -> "HNI-MP-02-01", "QTI-MP01-1" -> "QTI-MP-01-01",
      "Hostname_Radius" -> "Hostname_Noc", "HN-MP02-2" -> "HNI-MP-02-02", "HCM-MP02-1" -> "HCM-MP-02-01", "BDH-MP01-4" -> "BDH-MP-01-04",
      "HYN-MP01-3" -> "HYN-MP-01-03", "BDG-MP01-1-New" -> "BDG-MP-01-01", "YBI-BRAS01" -> "YBI-MP-01-01", "HN-MP02-7" -> "HNI-MP-02-07",
      "LGI-MP01-1" -> "LGI-MP-01-01", "7200-FCAM-09" -> "TNH-MP-01-01", "HCM-MP05-5" -> "HCM-MP-Backup-01", "DAH-MP02" -> "DAH-MP-01-02",
      "DTP-MP01-1" -> "DTP-MP-01-01", "STY-MP02" -> "STY-MP-01-02", "HP-MP01-NEW" -> "HPG-MP-01-01", "LDG-MP01-4" -> "LDG-MP-01-04",
      "DLK-MP01-1" -> "DLK-MP-01-01", "BDH-MP01-1" -> "BDH-MP-01-01", "HTH-MP02" -> "HTH-MP-01-02", "QBH-MP01-1" -> "QBH-MP-01-01",
      "CTO-MP01-2-NEW" -> "CTO-MP01-2-NEW", "KTM-MP01-2" -> "KTM-MP-01-02", "HYN-MP02" -> "HYN-MP-01-02", "CMU-MP01-1" -> "CMU-MP-01-01",
      "BNH-MP01-2" -> "BNH-MP-01-02", "HCM-QT-MP02-2" -> "HCM-MP-02-02", "BRA-MP01-2" -> "BRA-MP-01-02", "DNG-MP01-1" -> "DNG-MP-01-01",
      "HCM-MP04-1-NEW" -> "HCM-MP-04-01", "TVH-MP01-2" -> "TVH-MP-01-02", "HN-MP02-4" -> "HNI-MP-Backup-01", "BDG-MP01-2-New" -> "BDG-MP-01-02",
      "PTO-MP02" -> "PTO-MP-01-02", "HCM-MP01-2" -> "HCM-MP-01-02", "HCM-MP05-2" -> "HCM-MP-05-02", "NTN-MP01-2" -> "NTN-MP-01-02", "DNG-MP-2" -> "DNG-MP-01-02",
      "THA-MP01-4" -> "THA-MP-01-04", "PYN-MP01-1" -> "PYN-MP-01-01", "NAN-MP01-2" -> "NAN-MP-01-02", "HDG-MP01-2" -> "HDG-MP-01-02", "QNH-MP02" -> "QNH-MP-01-02",
      "CTO-MP01-1" -> "CTO-MP01-1-NEW", "LDG-MP01-1" -> "LDG-MP-01-01", "TQG-MP01-2" -> "TQG-MP-01-02", "HP-MP02-NEW" -> "HPG-MP-01-02", "BLC-MP01-2" -> "BLC-MP-01-02",
      "TGG-MP01-1" -> "TGG-MP-01-01", "KGG-MP01-2" -> "KGG-MP-01-02", "GLI-MP01-1" -> "GLI-MP-01-01", "THA-MP01-1" -> "THA-MP-01-01", "HD-BRAS01" -> "YBI-MP-01-01",
      "BTE-MP01-2" -> "BTE-MP-01-02", "DLK-MP01-3" -> "DLK-MP-01-03", "BDH-MP01-3" -> "BDH-MP-01-03", "NAN-MP01-4" -> "NAN-MP-01-04", "7200-FCAM-08" -> "TNH-MP-01-02",
      "GLI-MP01-4" -> "GLI-MP-01-04", "HCM-MP05-4" -> "HCM-MP-Backup-02", "NTG-MP01-02" -> "NTG-MP-01-02", "DAH-MP01" -> "DAH-MP-01-01", "BTN-MP01-1-NEW" -> "BTN-MP-01-01",
      "STY-MP01" -> "STY-MP-01-01", "TNN-MP02" -> "TNN-MP-01-02", "HDG-MP01-4" -> "HDG-MP-01-04", "QNH-MP04" -> "QNH-MP-01-04", "KTM-MP01-1" -> "KTM-MP-01-01",
      "HYN-MP01" -> "HYN-MP-01-01", "BRA-MP01-1" -> "BRA-MP-01-01", "KGG-MP01-4" -> "KGG-MP01-4", "QNI-MP01-2" -> "QNI-MP-01-02", "VPC-MP02" -> "VPC-MP-01-02",
      "VLG-MP01-2" -> "VLG-MP-01-02", "AGG-MP01-2" -> "AGG-MP-01-02", "LDG-MP01-3" -> "LDG-MP-01-03", "BNH-MP01-1" -> "BNH-MP-01-01", "HTH-MP01" -> "HTH-MP-01-01",
      "HUE-MP01-1-NEW" -> "HUE-MP-01-01", "QNM-MP01-1" -> "QNM-MP-01-01", "LSN-MP01" -> "LSN-MP-01-01", "TNH-MP01-2" -> "TNH-MP-01-02", "HCM-MP01-1" -> "HCM-MP-01-01",
      "BNH-MP01-4" -> "BNH-MP-01-04", "HN-MP03-2" -> "HNI-MP-03-02", "HN-MP05-2" -> "HNI-MP-05-02", "HCM-MP05-1" -> "HCM-MP-05-01", "QTI-MP01-2" -> "QTI-MP-01-02",
      "HN-MP01-2" -> "HNI-MP-01-02", "IXIA" -> "YBI-MP-01-02", "THA-MP01-3" -> "THA-MP-01-03", "PTO-MP01" -> "PTO-MP-01-01", "NAN-MP01-1" -> "NAN-MP-01-01",
      "TVH-MP01-1" -> "TVH-MP-01-01", "HN-MP02-3" -> "HNI-MP-Backup-02", "HCM-MP03-1-NEW" -> "HCM-MP-03-01", "QNH-MP01" -> "QNH-MP-01-01",
      "BTN-MP01-2-NEW" -> "BTN-MP-01-02", "TNN-MP01-NEW" -> "TNN-MP-01-01", "HN-MP02-6" -> "HNI-MP-02-06", "KGG-MP01-1" -> "KGG-MP-01-01",
      "HDG-MP01-1" -> "HDG-MP-01-01", "BLC-MP01-1" -> "BLC-MP-01-01", "DNI-MP01-1-NEW" -> "DNI-MP-01-01", "TNN-MP04" -> "TNN-MP-01-04",
      "BTE-MP01-1" -> "BTE-MP-01-01", "HUE-MP01-2-NEW" -> "HUE-MP-01-02", "NTG-MP01-01" -> "NTG-MP-01-01", "HCM-MP04-2" -> "HCM-MP-04-02",
      "NTN-MP01-1" -> "NTN-MP-01-01", "BPC-MP01-2" -> "BPC-MP-01-02", "GLI-MP01-3" -> "GLI-MP-01-03", "HYN-MP01-4" -> "HYN-MP-01-04",
      "HCM-MP06-2" -> "HCM-MP-06-02", "7200-FCAM-07" -> "BPC-MP-01-02"))
    //val bGson = sc.broadcast[Gson](new Gson())
    // PG properties :
    val jdbcUrl = PostgresIO.getJDBCUrl(postgresConfig)
    println(jdbcUrl)
    val bJdbcURL = sc.broadcast(jdbcUrl)
    //FIXME :
    // Ad-hoc fixing
    val pgProperties    = new Properties()
    pgProperties.setProperty("driver","org.postgresql.Driver")
    val bPgProperties   = sc.broadcast(pgProperties)
 /*   val bWindowDuration = sc.broadcast(windowDuration)
    val bSlideDuration  = sc.broadcast(slideDuration)
    val bConLogParser   = sc.broadcast(conLogParser)*/

    //val lookupHostName: (String => String) = (arg: String) =>{
    //  lookupCache.value.getOrElse(arg,"N/A")
    //}
    //val sqlLookup = org.apache.spark.sql.functions.udf(lookupHostName)

    val objectConnLogs: DStream[ConnLogLineObject] = lines.transform(extractValue(bConLogParser,bBrasNameLookUp))

    // SAVE TO CASSANDRA
/*        objectConnLogs.saveToCassandra(cassandraConfig("keySpace").toString,
                                       cassandraConfig("table").toString,
                                       SomeColumns("time","session_id","connect_type","name","content1","content2"))*/
    // Save to ES :
    try{
      import storage.es.ElasticSearchDStreamWriter._
      // event var not work
      //var today = org.joda.time.DateTime.now().toString("yyyy-MM-dd")
      // maybe def work

      //Save conn log to ES
      //objectConnLogs.persistToStorageDaily(Predef.Map[String,String]("indexPrefix" -> "radius-connlog_new","type" -> "connlog"))
      objectConnLogs.persistToStorage(Predef.Map[String,String]("index" -> ("radius-" + org.joda.time.DateTime.now().toString("yyyy-MM-dd")),"type" -> "connlog"))
      //objectConnLogs.persistToStorage(Predef.Map[String,String]("index" -> ("radius-test-" + today),"type" -> "connlog"))
    } catch {
      case e: Exception => System.err.println("UncatchException occur when save connlog to ES : " +  e.getMessage)
      case _ => println("Ignore !")
    }

    val brasInfoDStream: DStream[ConnLogLineObject] = objectConnLogs.transform(removeRject).cache()

    //Sorry, it was 7PM, i was too lazy to code. so i did too much hard code here :)).
    /*val connType = objectConnLogs
      .map(conlog => (conlog.connect_type,1))
      .reduceByKeyAndWindow((a:Int,b:Int)=>a+b,bWindowDuration.value,bSlideDuration.value)
      //.reduceByKeyAndWindow( _ + _ , _ -_ , bWindowDuration.value,bSlideDuration.value)
      .transform(skipEmptyWordCount)  //Uncomment this line to remove empty wordcoutn such as : SignInt : Count 0
*/
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
      //Save connCounting to ES
/*    connType.transform(toWouldCountObject)
      .foreachRDD{rdd =>

        //Save Conn Counting to Es
        var typeName  = "conn_counting"
        rdd.saveToEs("radius-" + org.joda.time.DateTime.now().toString("yyyy-MM-dd") + "/" + typeName)

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
    }*/

    brasInfoDStream.foreachRDD({
      (rdd: RDD[ConnLogLineObject],time_ : Time) =>
        // Get sparkContext from rdd
        ///val context = rdd.sparkContext
        // Get the singleton instance of SparkSession
        //val sparkSession = SparkSessionSingleton.getInstance(context.getConf)
        //val sparkSession = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        //import sparkSession.implicits._
        // Convert RDD[ConnLogObject] to RDD
        // Signin - logoff logs have bras name in content 1 -> filter out reject.
        /////////////////////////////////////// BRAS ////////////////////////////////////////////////////////////
        ////////////////////////////////////////////////////////////////////////////////////////////////////////
        //Deal with time. minutes.
        val logOff = rdd.filter(x => x.connect_type == ConnectTypeEnum.LogOff.toString)
                        .map{ ob => ((ob.content1,ob.time.substring(0,16).replace("T"," ")),ob.name)}
                        .groupByKey().map(x => (x._1,x._2.toSet.toList))
        //Save logOff to Redis
        logOff.foreachPartition{
          partition =>
            //val clients = new RedisClientPool("172.27.11.141",6373)
            val clients = RedisClientFactory.getOrCreateClient(("172.27.11.141", 6373))
            partition.foreach{tuple =>

              clients.withClient{client =>
                // Key : Brasid-time - Value : List
                tuple._2.foreach(client.lpush(tuple._1._1+ "-"+tuple._1._2,_))
                //client.lpush(tuple._1,tuple._2)
                client.expire(tuple._1,300)
              }
            }

        }



        val brasInfo = rdd.toDF("time","session_id","connect_type","name","content1","line","card","port","olt","portpon","macadd","vlan","serialonu")
                            .cache()
        brasInfo.createOrReplaceTempView("bras_info")
        //TODO : Mapping hostname -brastogether.
        // Select name and BrasName where connect type == signin
        val brasAndHost: DataFrame = ss.sql("SELECT content1 , olt , portpon , CONCAT(olt,'/',portpon) as host_endpoint FROM bras_info").where(col("connect_type") === "SignIn")
                                      .filter("host_endpoint != 'n/a/n/a'")
                                    //.withColumn("host",sqlLookup(col("name")))
                                    .withColumnRenamed("content1","bras_id")
                                    .dropDuplicates("host_endpoint")

        //Save to cassandra -- table - keyspace - cluster.
        //println(" BRAS AND HOST")
        //brasAndHost.show()
        //TODO : HARDCODE !!!!!! CASSANDRA
        //brasAndHost.write.mode("append").cassandraFormat("brashostmapping","radius","test").save()
        //TODO : Migrate to Postgres.
        //TODO Code Upsert function
        // Append only will be cause of conflict.

        try{
          // @depricate
          //PostgresIO.writeToPostgres(ss, brasAndHost, bJdbcURL.value, "brashostmapping", SaveMode.Overwrite, bPgProperties.value)
          brasAndHost.foreachPartition{ batch =>
            val conn: Connection = DriverManager.getConnection(bJdbcURL.value)
            val st: PreparedStatement = conn.prepareStatement("INSERT INTO brashostmapping(bras_id,olt,portPON,host_endpoint) " +
              " VALUES (?,?,?,?)" +
              " ON CONFLICT (host_endpoint) DO UPDATE  " +
              " SET bras_id = excluded.bras_id, " +
              " olt = excluded.olt, " +
              " portPON = excluded.portPON ;"
            )
            // 300 : size of batch : Number of rows you want per batch.

            batch.grouped(300).foreach {session =>
              session.foreach{x =>
                st.setString(1,x.getString(0))
                st.setString(2,x.getString(1))
                st.setString(3,x.getString(2))
                st.setString(4,x.getString(3))
                st.addBatch()
              }
              st.executeBatch()
            }
            // Can we add try catch here ??? -
            // TODO WARNING!!! this produce connection leak. /!\
            conn.close()
            logger.info(s"Save brashost mapping successfully")
          }

        }catch{
          case e: SQLException => System.err.println("SQLException occur when save brashostmapping : " + e.getSQLState + " " + e.getMessage)
          case e: Exception => System.err.println("UncatchException occur when save brashostmapping : " +  e.getMessage)
          case _ => println("Ignore !")
        }

        brasAndHost.unpersist()

/*
        val timeFunc: (AnyRef => String) = (arg: AnyRef) => {
          getCurrentTime()
        }
        val sqlTimeFunc = udf(timeFunc)*/

        ///////////////////// COUNT BY PORT //////////////////////////////////////////////////////////////
        val brasCountByPort = brasInfo.select("name","connect_type","port")
                                        .groupBy(col("port"),col("connect_type"))
          .agg(count(col("name")).as("count_by_port"),countDistinct(col("name")).as("count_distinct_by_port"))
          .cache()

        val brasCountByPortTotalPivot =  brasCountByPort.groupBy("port").pivot("connect_type",bConnectType.value)
          .agg(expr("coalesce(first(count_by_port),0)"))
          .withColumnRenamed("SignIn","signin_total_count_by_port")
          .withColumnRenamed("LogOff","logoff_total_count_by_port")

        val brasCountByPortDistinctPivot = brasCountByPort.groupBy("port").pivot("connect_type",bConnectType.value)
          .agg(expr("coalesce(first(count_distinct_by_port),0)"))
          .withColumnRenamed("SignIn","signin_distinct_count_by_port")
          .withColumnRenamed("LogOff","logoff_distinct_count_by_port")

        val brasCountByPortPivot: DataFrame = brasCountByPortTotalPivot.join(brasCountByPortDistinctPivot,"port")
          //.withColumn("time",sqlTimeFunc(col("content1")))
          .withColumn("time",org.apache.spark.sql.functions.current_timestamp())
        brasCountByPortPivot.createOrReplaceTempView("brasCountByPortPivot")

        val result_cb_port = ss.sql("SELECT *, split(port, '/')[0] as bras_id," +
          " split(port, '/')[1] as line_ol, split(port, '/')[2] as card_ol,split(port, '/')[3] as port_ol FROM brasCountByPortPivot")

        //TODO SAVE TO POSTGRES.

        try{
          PostgresIO.writeToPostgres(ss, result_cb_port, bJdbcURL.value, "bras_count_by_port", SaveMode.Append, bPgProperties.value)
        }catch{
          case e: SQLException => System.err.println("SQLException occur when save bras_count_by_port : " + e.getSQLState + " " + e.getMessage)
          case e: Exception => System.err.println("UncatchException occur when save bras_count_by_port : " +  e.getMessage)
          case _ => println("Ignore !")
        }



        //////////////////////////// COUNT BY CARD ///////////////////////////////////////////////////////////
        val brasCountByCard = brasInfo.select("name","connect_type","card")
          .groupBy(col("card"),col("connect_type"))
          .agg(count(col("name")).as("count_by_card"),countDistinct(col("name")).as("count_distinct_by_card"))
          .cache()

        val brasCountByCardTotalPivot =  brasCountByCard.groupBy("card").pivot("connect_type",bConnectType.value)
          .agg(expr("coalesce(first(count_by_card),0)"))
          .withColumnRenamed("SignIn","signin_total_count_by_card")
          .withColumnRenamed("LogOff","logoff_total_count_by_card")

        val brasCountByCardDistinctPivot = brasCountByCard.groupBy("card").pivot("connect_type",bConnectType.value)
          .agg(expr("coalesce(first(count_distinct_by_card),0)"))
          .withColumnRenamed("SignIn","signin_distinct_count_by_card")
          .withColumnRenamed("LogOff","logoff_distinct_count_by_card")

        val brasCountByCardPivot: DataFrame = brasCountByCardTotalPivot.join(brasCountByCardDistinctPivot,"card")
          //.withColumn("time",sqlTimeFunc(col("content1")))
          .withColumn("time",org.apache.spark.sql.functions.current_timestamp())
        brasCountByCardPivot.createOrReplaceTempView("brasCountByCardPivot")

        val result_cb_card = ss.sql("SELECT *,split(card, '/')[0] as bras_id, " +
          "split(card, '/')[1] as line_ol, split(card, '/')[2] as card_ol FROM brasCountByCardPivot")

        //TODO SAVE TO POSTGRES
        try{
          PostgresIO.writeToPostgres(ss, result_cb_card, bJdbcURL.value, "bras_count_by_card", SaveMode.Append, bPgProperties.value)
        }catch{
          case e: SQLException => System.err.println("SQLException occur when save bras_count_by_card : " + e.getSQLState + " " + e.getMessage)
          case e: Exception => System.err.println("UncatchException occur when save bras_count_by_card : " +  e.getMessage)
          case _ => println("Ignore !")
        }



       ////////////////////////////// LineCard ///////////////////////////////////


        val brasCountByLine = brasInfo.select("name","connect_type","line")
          .groupBy(col("line"),col("connect_type"))
          .agg(count(col("name")).as("count_by_line"),countDistinct(col("name")).as("count_distinct_by_line"))
          .cache()

        val brasCountByLineTotalPivot =  brasCountByLine.groupBy("line").pivot("connect_type",bConnectType.value)
          .agg(expr("coalesce(first(count_by_line),0)"))
          .withColumnRenamed("SignIn","signin_total_count_by_line")
          .withColumnRenamed("LogOff","logoff_total_count_by_line")

        val brasCountByLineDistinctPivot = brasCountByLine.groupBy("line").pivot("connect_type",bConnectType.value)
          .agg(expr("coalesce(first(count_distinct_by_line),0)"))
          .withColumnRenamed("SignIn","signin_distinct_count_by_line")
          .withColumnRenamed("LogOff","logoff_distinct_count_by_line")

        val brasCountByLinePivot: DataFrame = brasCountByLineTotalPivot.join(brasCountByLineDistinctPivot,"line")
          //.withColumn("time",sqlTimeFunc(col("content1")))
          .withColumn("time",org.apache.spark.sql.functions.current_timestamp())

        brasCountByLinePivot.createOrReplaceTempView("brasCountByLinePivot")
        val result_cb_line = ss.sql("SELECT *,split(line, '/')[0] as bras_id, split(line, '/')[1] as line_ol FROM brasCountByLinePivot ")

        //TODO SAVE TO POSTGRES
        try{
          PostgresIO.writeToPostgres(ss, result_cb_line, bJdbcURL.value, "bras_count_by_line", SaveMode.Append, bPgProperties.value)
        }catch{
          case e: SQLException => System.err.println("SQLException occur when save bras_count_by_line : " + e.getSQLState + " " + e.getMessage)
          case e: Exception => System.err.println("UncatchException occur when save bras_count_by_line : " +  e.getMessage)
          case _ => println("Ignore !")
        }

        // UNPERSIT
        brasCountByLine.unpersist()
        brasCountByCard.unpersist()
        brasCountByPort.unpersist()

        //////////////////////////// Bras ///////////////////////////////////////
        brasInfo.unpersist()



    })

    brasInfoDStream.window(bWindowDuration.value,bSlideDuration.value).foreachRDD({batch =>
      val brasInfo = batch.toDF("time","session_id","connect_type","name","content1","line","card","port","olt","portpon","macadd","vlan","serialonu")
        .cache()
      brasInfo.createOrReplaceTempView("bras_info")
      val brasCount = brasInfo.select("name","connect_type","content1")
        .groupBy(col("content1"),col("connect_type"))
        .agg(count(col("name")).as("count_by_bras"),countDistinct(col("name")).as("count_distinct_by_bras"))
        .cache()

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
        .cache()

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

        val brasCountObjectRDDTop50 = top50Sum.rdd.map{ row =>
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

        val brasCountObjectRDD = brasCountPivot.rdd.map{ row =>
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

        // Make rdd from sequence then save to postgres
        //SAVE TO ES
        //var brasCountIndex = "count_by_bras-"+org.joda.time.DateTime.now().toString("yyyy-MM-dd") +"-" + "%02d".format(Calendar.getInstance().get(Calendar.HOUR_OF_DAY))
        // SAVE TO  ES v1.1 - USING FOR BRAS OUTLY
        //var brasCountType = "bras_count"
        //brasCountObjectRDD.saveToEs("count_by_bras-"+org.joda.time.DateTime.now().toString("yyyy-MM-dd") +"-" + "%02d".format(Calendar.getInstance().get(Calendar.HOUR_OF_DAY)) + "/" + brasCountType)
        // Send to Top  50 To PowerBI
        brasCountObjectRDDTop50.foreachPartition{partition =>
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
        // Send All bras count to Kafka // Consider remove this one.
        // TODO Replace with directly save data to postgres.
        /*          brasCountObjectRDD.foreachPartition{partition =>
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
                  }*/
        //Send To Kafka - For Anomaly detection
        //Save Bras count to postgres



        try{
          PostgresIO.writeToPostgres(ss,brasCountPivot,bJdbcURL.value,"bras_count",SaveMode.Append,bPgProperties.value)
        }catch{
          case e: SQLException => System.err.println("SQLException occur when save bras_count : " + e.getSQLState + " " + e.getMessage)
          case e: Exception => System.err.println("UncatchException occur when save bras_count : " +  e.getMessage)
          case _ => println("Ignore !")
        }




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
      /*  val infInfo   =  rdd.toDF("time","session_id","connect_type","name","content1","content2")
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
          .withColumn("time",org.apache.spark.sql.functions.current_timestamp()).cache()*/
      //.withColumn("time",org.apache.spark.sql.functions.current_timestamp())
      //.withColumnRenamed("content1","bras_id")

      //println(s"========= $time_ =========")
      /*if(infCountPivot.count() > 0) {
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
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        //SAVE TO ES
        infCountObjectRDD.saveToEs("count_by_inf-"+org.joda.time.DateTime.now().toString("yyyy-MM-dd") +"-" + "%02d".format(Calendar.getInstance().get(Calendar.HOUR_OF_DAY)) + "/" + infCountType)
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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


    }*/
      //infCountPivot.unpersist()
      brasCount.unpersist()
      brasCountPivot.unpersist()
      brasInfo.unpersist()
    })

  }


  def toLowerCase   = (lines: RDD[String]) => lines.map(words => words.toLowerCase)
  //FIXME !!!
  def extractValue  = (parser: Broadcast[ConnLogParser],brasNameLookUp: Broadcast[Predef.Map[String,String]]) => (lines: RDD[String]) =>
    lines.map{ line =>
      val parsedObject = parser.value.extractValues_newFormat(line).getOrElse(None)
      //val parsedObject = parser.value.extractValues(line).getOrElse(None)
      parsedObject match{
        case Some(x) => x.asInstanceOf[ConnLogLineObject]
        case _ => None
      }
      parsedObject
    }.filter(x => x != None).map{ob =>
      //ob.asInstanceOf[ConnLogLineObject]
      val cll = ob.asInstanceOf[ConnLogLineObject]
      /*val cllMapped = new ConnLogLineObject(cll.time,cll.session_id,cll.connect_type,cll.name,brasNameLookUp.value.getOrElse(cll.content1,"n/a"),cll.lineCards,
        cll.card,cll.port,cll.olt,cll.portPON,cll.macAdd,cll.vlan,cll.serialONU)
      cllMapped*/
      cll
    }.filter(x => x.content1 != "n/a")

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

  def removeRject = (streams: RDD[ConnLogLineObject]) => streams.filter(line => line.connect_type != ConnectTypeEnum.Reject.toString)





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
/*
object SaveToCassTest{

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkConfig = new SparkConf().set( "spark.cassandra.connection.host" , "172.27.11.156")
      .set("spark.cassandra.output.batch.size.rows" , "auto")
      .set("spark.mongodb.output.uri", "mongodb://172.27.11.146:27017/radius.outlier")
    val sparkSession = SparkSession.builder().appName("wirteCTest").master("local[2]").config(sparkConfig).getOrCreate()
    //val rdd = sparkSession.sparkContext.parallelize(Seq(("Bras1","Host1"),("Bras2","Host2"),("Bras3","Host3"),("Bras4","Host4"),("Bras5","Host5")))
    val rdd = sparkSession.sparkContext.parallelize(Seq(("BrasC","HostCC"),("BrasB","HostBB")))
    //val rdd2 = sparkSession.sparkContext.parallelize(Seq(()))
    val rdd2 = sparkSession.sparkContext.parallelize(Seq(("BrasC","MCC")))
    val mongordd = sparkSession.sparkContext.parallelize(Seq(("LDG-MP01-3",2,2,3,3,"normal")))
    val context = sparkSession.sparkContext
    import sparkSession.implicits._
    val mongodf = mongordd.toDF("bras_id","logoff_distinct_count","logoff_total_count","signin_distinct_count","signin_total_count","label").withColumn("time",org.apache.spark.sql.functions.current_timestamp())

    mongodf.show
    mongodf.write.mode("append").mongo(WriteConfig(Map("collection"->"outlier"),Some(WriteConfig(context))))



/*    val df = rdd.toDF("bras_id","host")
    val schema = df.schema
    val empty = sparkSession.emptyDataFrame
    val sparkConetx = sparkSession.sparkContext

    val df2 = rdd2.toDF("bras_id","mm")
    val joined = df.join(df2,"bras_id")
    val leftJoind = df.join(df2,Seq("bras_id"),"left_outer")
    joined.show()
    leftJoind.show()
    val normal: (String => String) = (arg: String) =>{
      "true"
    }
    val df3 = df.join(empty,"bras_id")
    df3.show()

    val sqlLookup = org.apache.spark.sql.functions.udf(normal)
    val test = leftJoind.withColumn("test",sqlLookup(col("bras_id")))
    test.show()*/
   /* df.write.mode("append").cassandraFormat("brashostmapping","radius","test").save()
    // TODO never use overwrite mode.
    //df.write.mode("overwrite").cassandraFormat("brashostmapping","radius","test").save()
    val config = ConnJobConfig()
    val postgresConfig: Predef.Map[String, String] =  config.postgresStorage
    // get jdbc url.
    val jdbcUrl = PostgresIO.getJDBCUrlForRead(postgresConfig)
    //FIXME :
    // Ad-hoc fixing
    val pgProperties = new Properties()
    pgProperties.setProperty("driver","org.postgresql.Driver")

    //
    // Load name and INF host from database -> df
    val lookupDf  = PostgresIO.selectedByColumn(sparkSession,jdbcUrl,"internet_contract",List("name","host"),pgProperties)
    lookupDf.show()*/

  }
}
*/