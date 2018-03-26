package com.ftel.bigdata.radius.anomaly

import com.ftel.bigdata.radius.classify.{ConLog, Parser}
import com.ftel.bigdata.radius.postgres.PostgresUtil
import com.ftel.bigdata.radius.streaming.RadiusMessage
import com.ftel.bigdata.radius.utils.{Math, SlackUtil}
import com.ftel.bigdata.utils.{DateTimeUtil, JsonObject, Parameters}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql.sparkDataFrameFunctions
//import org.scalacheck.Gen.Parameters

object ADStreaming {
  val SPARK_MASTER = "yarn"
  val DATE_FORMAT = "yyyy-MM-dd HH:mm"
  val OUTPUT_PATH = "/data/radius"

  def main(args: Array[String]): Unit = {
    run()
  }

  def createSparkConf(maxRate: Int): SparkConf = {
    val sparkConf = new SparkConf().setAppName("Anomaly Detect Driver").setMaster(SPARK_MASTER)
    /**
      * Configure for Spark.
      */
    //sparkConf.set("spark.scheduler.mode", "FAIR");
    //sparkConf.set("spark.default.parallelism", "8");
    //sparkConf.set("spark.task.cpus", "4");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
    sparkConf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    //    sparkConf.set("spark.streaming.concurrentJobs", "4")
    sparkConf.set("spark.streaming.concurrentJobs", "1")
    sparkConf.set("spark.streaming.unpersist", "true")

    sparkConf.set("log4j.configuration", "org/apache/spark/log4j.properties")

    /**
      * Configure for Kafka
      */
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", maxRate.toString)
    //    sparkConf.set("spark.streaming.kafka.consumer.cache.enabled", "false") // solution for Issue 19185 - "KafkaConsumer is not safe for multi-threaded access"

    //sparkConf.set("spark.driver.allowMultipleContexts", "true")

    /**
      * Config Elasticsearch
      */
    //sparkConf.set("es.resource", esIndex + "/" + esType)
    sparkConf.set("es.nodes", "172.27.11.156" + ":" + "9200") // List IP/Hostname/host:port
    //sparkConf.set("es.port", port)  // apply for host in es.nodes that do not have any port specified
    // For ES version 5.x, Using 'create' op will error if you don't set id when create
    // To automatic ID generate, using 'index' op
    sparkConf.set("es.write.operation", "index")
    sparkConf.set("es.batch.size.bytes", "10mb")
    sparkConf.set("es.batch.size.entries", Integer.toString(1000)) // default 1000
    sparkConf.set("es.batch.write.refresh", "true")
    sparkConf
  }

  private def loadThreshold(sc: SparkContext): (Long, Long) = {
    sc.textFile("/data/radius/threshold-week")
      .map(_.split("\t"))
      .map(x => (x(0).toLong, x(1).toLong))
      .first()
  }

  private def run() {
    // Kafka Information
    val brokers: String = "172.27.11.75:9092,172.27.11.80:9092,172.27.11.85:9092"
    val topic: String = "radius"
    val fromOffsets: Map[TopicPartition, Long] = null //readCheckPoint(ssc.sparkContext)
    val maxRate: Int = 1000
    val duration: Int = 60

    val sparkConf = createSparkConf(maxRate)
    val ssc = new StreamingContext(sparkConf, Seconds(duration))
    ssc.sparkContext.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())

    val thresholds = loadThreshold(ssc.sparkContext)
    val SThresh = thresholds._1
    val LThresh = thresholds._2

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "radius-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topicPartitions = Array(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic, 2))

    val consumerStrategy = if (fromOffsets == null) {
      println("Run with latest offset")
      //      Subscribe[String, String](Array(topic), kafkaParams)
      Assign[String, String](topicPartitions, kafkaParams)
    } else {
      println("Run with offset: " + fromOffsets.values.mkString("\t"))
      //      Subscribe[String, String](fromOffsets.keys.map(_.topic()).toList, kafkaParams, fromOffsets)
      Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets)
    }

    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, consumerStrategy)

    val sqlContext = SparkSession.builder().getOrCreate()
    import sqlContext.implicits._

    val dstream = stream.map(x => new RadiusMessage(new JsonObject(x.value()), x.topic(), x.partition(), x.offset()))
        .map(x => Parser.parse(x.message, x.timeStamp))
        .window(Seconds(60*15))
        .cache()

    /**
      * Group by Bras_id every minute
      * Sum SignIn - LofOff
      */

    dstream.foreachRDD(rdd => {
      val act_user = PostgresUtil.getFromPostgres("active_user").cache()

      val t = DateTimeUtil.create(System.currentTimeMillis()/1000).minusMinutes(1).toString(DATE_FORMAT)

      val con = rdd.filter(x => x.isInstanceOf[ConLog])
        .map(x => x.asInstanceOf[ConLog])
        .filter(_.typeLog != ConLog.REJECT._2)
        .map(x => {
          val signin = if (x.typeLog == ConLog.SIGNIN._2) 1L else 0L
          val logoff = if (x.typeLog == ConLog.LOGOFF._2) 1L else 0L
          (DateTimeUtil.create(x.timestamp/1000).toString(DATE_FORMAT), x.nasName.toUpperCase) -> (signin, logoff)
        })
        .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2), 32)
        .map(x => (x._1._1, x._1._2, x._2._1, x._2._2))
        .toDF("date_time", "bras_id", "signIn", "logOff")

      calculateMetrics(con, t, act_user, SThresh, LThresh)
    })

    ssc.start()
    ssc.awaitTermination()

  }

  def calculateMetrics(df: DataFrame, t: String, act_user: DataFrame, sThresh: Long, lThresh: Long) = {
    val sqlContext = SparkSession.builder().getOrCreate()
    import sqlContext.implicits._

    /**
      * Filter 0 value then cache
      */
    val dfTemp = df.filter($"signIn" =!= 0 && $"logOff" =!= 0).cache()

//    dfTemp.select(max($"signIn").alias("signIn"))
//      .join(dfTemp, Seq("signIn"))
//      .show()
//
//    dfTemp.select(max($"logOff").alias("logOff"))
//      .join(dfTemp, Seq("logOff"))
//      .show()

    /**
      * Calculate IQR in 15 min range
      */
    val prev15m = DateTimeUtil.create(t, DATE_FORMAT).minusMinutes(15).toString(DATE_FORMAT)
    val previousBrasLog = dfTemp.filter($"date_time".between(prev15m, t))
      .rdd
      .map(x => (x.getString(1)) -> (x.getLong(2), x.getLong(3)))
      .groupByKey()
      .filter(_._2.size > 0)
      .mapValues(x => Math.IQR(x))
      .map(x => (x._1, x._2._1, x._2._2))
      .toDF("bras_id", "SL_IQR", "LS_IQR")

    /**
      * Calculate Rate
      */
    val presentBrasLog = dfTemp.filter($"date_time" === t)
      .rdd
      .map(x => (x.getString(1), x.getLong(2), x.getLong(3), x.getLong(2)/x.getLong(3).toDouble, x.getLong(3)/x.getLong(2).toDouble))
      .toDF("bras_id", "signIn", "logOff", "SL_Rate", "LS_Rate")

    /**
      * Join IQR + Rate + Active
      */
    val joined = presentBrasLog.join(previousBrasLog, Seq("bras_id"))
      .join(act_user, Seq("bras_id"))
    //  bras_id|signIn|logOff| SL_Rate| LS_Rate|SL_IQR|LS_IQR | active_users

    /**
      *  Check conditions
      */
    val anomalies = joined.rdd
      .map{case (row) => (row.getString(0), row.getLong(1), row.getLong(2),
          row.getDouble(3), row.getDouble(4), row.getLong(5), row.getLong(6), row.getInt(7))
        }
      .map{case (bras_id, s, l, slRate, lsRate, slIQR, lsIQR, activeUsers) => {
        if (slRate > lsRate) {
          if (s > sThresh && slRate > slIQR && s/activeUsers.toDouble > 0.03) {
            (bras_id, s, l, slRate, lsRate, slIQR, lsIQR, activeUsers, "outlier")
          } else {
            (bras_id, s, l, slRate, lsRate, slIQR, lsIQR, activeUsers, "normal")
          }
        } else {
          if (l > lThresh && lsRate > lsIQR && l/activeUsers.toDouble > 0.03) {
            (bras_id, s, l, slRate, lsRate, slIQR, lsIQR, activeUsers, "outlier")
          } else {
            (bras_id, s, l, slRate, lsRate, slIQR, lsIQR, activeUsers, "normal")
          }
        }
      }}
      .toDF("bras_id", "signIn", "logOff", "SL_Rate", "LS_Rate", "SL_IQR", "LS_IQR", "active_users", "label")
      .persist()

    anomalies
      .withColumn("date_time", lit(DateTimeUtil.create(t, DATE_FORMAT).toString(Parameters.ES_5_DATETIME_FORMAT)))
      .select($"date_time", $"bras_id", $"signIn", $"logOff", $"SL_Rate", $"LS_Rate", $"SL_IQR", $"LS_IQR", $"active_users", $"label")
      .saveToEs(s"monitor-radius-${DateTimeUtil.create(t, DATE_FORMAT).toString(DateTimeUtil.YMD)}/docs")

    /**
      * Output if found
      */
    if (anomalies.filter($"label" === "outlier").count() > 0) {
      SlackUtil.alert("dungvo", "Bras Anomaly Detected", "Number of bras: " + anomalies.filter($"label" === "outlier").count(), 1)
      val fullInfo = PostgresUtil.getInformation(anomalies.filter($"label" === "outlier"), t)
      PostgresUtil.writeOutlierToPostgres(fullInfo)
      anomalies.show()
//      anomalies.coalesce(1).write.csv(s"$OUTPUT_PATH/anomalies/$date/$hm")
    }
  }
}
