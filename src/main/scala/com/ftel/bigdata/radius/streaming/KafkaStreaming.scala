package com.ftel.bigdata.radius.streaming

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.dstream.DStream
import com.ftel.bigdata.utils.DateTimeUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext

object KafkaStreaming {

  /**
   * brokers: list host:port, split by comma
   * topic: name of topic in kafka
   * offsetsString: format (partiton1:offset1,partiton2:offset2, ...), if null, streaming will get latest message
   * maxRate: max message on each second
   * durations: unit is second
   *     => IF durations is 5, maxRate is 100 => each duration will process for 5 * 100 = 500 messages
   * f: function for process message from kafka
   */
  def run(
      ssc: StreamingContext,
      brokers: String,
      topic: String,
      offsetsString: String,
      //readOffset: SparkContext => Map[TopicPartition, Long],
      maxRate: Int,
      durations: Int,
      messageHandler: (SparkContext, DStream[RadiusMessage]) => Unit) {

    //PropertyConfigurator.configure(Driver.getClass.getResourceAsStream("/org/apache/spark/log4j.properties"))
    //Logger.getRootLogger.setLevel(Level.OFF)

    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)

    // Kafka Information
//    val brokers: String = args(0)
//    val topic: String = args(1)
    val offsets: Array[String] = if (offsetsString == null) Array() else offsetsString.split(",") //args(2).split(",")
//    val maxRate: Int = args(3).toInt
//    val durations: Int = args(4).toInt
    //val log = args(5)

    //val closeListPath = args(5)
    //val host = args(6)
    //val port = args(7).toInt
    //PropertyConfigurator.configure(log)
    /*
    log match {
      case "info" => Logger.getRootLogger.setLevel(Level.ERROR)
      case "debug" => Logger.getRootLogger.setLevel(Level.DEBUG)
      case "error" => Logger.getRootLogger.setLevel(Level.ERROR)
      case "off" => Logger.getRootLogger.setLevel(Level.OFF)
      case _ => Logger.getRootLogger.setLevel(Level.INFO)
    }
    */

    val fromOffsets: Map[TopicPartition, Long] = offsets.map { x =>
      val arr = x.split(":")
      val partition = arr(0).toInt
      val offset = arr(1).toLong
      val topicPartition: TopicPartition = new TopicPartition(topic, partition)
      (topicPartition, offset)
    }.toMap

    
    
    //val fromOffsets = readOffset(ssc.sparkContext)
    //val kafkaParams: Map[String, String] = Map[String, String]("metadata.broker.list" -> brokers)
    //val messageHandler = (mam: MessageAndMetadata[String, String]) => new MessageIn(mam.topic, mam.partition, mam.offset, mam.message)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "radius-group",
      "auto.offset.reset" -> "latest", // auto.offset.reset: smallest|latest
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array(topic)
    val consumerStrategy = if (fromOffsets.isEmpty) {
      println("Run with latest offset")
      Subscribe[String, String](topics, kafkaParams)
    } else {
      println("Run with offset:")
      fromOffsets.foreach(x => println(x))
      Subscribe[String, String](topics, kafkaParams, fromOffsets)
    }
    
    //val consumerStrategy = Subscribe[String, String](topics, kafkaParams, fromOffsets)

    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, consumerStrategy)//.cache()
    
    val messages = stream.map(x => new RadiusMessage(new JsonObject(x.value()), x.topic(), x.partition(), x.offset())).cache()

    //messages.print(10)
    messageHandler(ssc.sparkContext, messages)
    //offsetHandler(stream)

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  
  
  
}