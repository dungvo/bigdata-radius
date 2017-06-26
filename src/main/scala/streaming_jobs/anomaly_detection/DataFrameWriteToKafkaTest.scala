package streaming_jobs.anomaly_detection

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.util.Properties

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.ForeachWriter
import core.KafkaProducerFactory
import core.sinks.{KafkaDStreamSink, KafkaDStreamSinkExceptionHandler, KafkaRDDSink}
import org.apache.spark.TaskContext
import java.util.UUID
/**
  * Created by hungdv on 11/06/2017.
  */

object WriteDFToKafka{

  val appName = "test"
  val master = "local[2]"
  def main (args: Array[String] ): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().appName(appName).master(master).getOrCreate()
    import sparkSession.implicits._
    val rdd = sparkSession.sparkContext.makeRDD(Seq(("1",2,"a"),("2",3,"b"),("3",2,"c")))
    val df = rdd.toDF("id","number","char")
    df.show()
    val df2 = df.withColumn("time",org.apache.spark.sql.functions.current_timestamp())
    df2.show(false)
   /* val topic = "output"
    val bTopic = sparkSession.sparkContext.broadcast(topic)
    val brokers = "localhost:9200"
    val producerConfig = Map(
      "bootstrap.servers" -> "localhost:9092",
      "acks"              -> "all",
        //"buffer.memory"     -> "8388608",
      "block.on.buffer.full"-> "true",
      "retries"           -> "2147483647",
      "retry.backoff.ms"  -> "1500",
      "key.deserializer"  -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"-> "org.apache.kafka.common.serialization.StringDeserializer",
      "key.serializer"    -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer"  -> "org.apache.kafka.common.serialization.StringSerializer"
   )
   val rddFromDF: RDD[String] = df.rdd.map{ row =>
     val string = row.getAs[String]("id") + row.getAs[Int]("number") + row.getAs[String]("char")
     string
   }
    rddFromDF.foreachPartition{
      partition =>
        val producer: KafkaProducer[String,String] = KafkaProducerFactory.getOrCreateProducer(producerConfig)
        val context = TaskContext.get()
        val callback = new KafkaDStreamSinkExceptionHandler
        //val logger = Logger.getLogger(getClass)
        //logger.debug(s"Send Spark partition: ${context.partitionId()} to Kafka topic in [anomaly]")
        partition.map{string =>
          val record = new ProducerRecord[String,String](bTopic.value,"hung",string)
          callback.throwExceptionIfAny()
          producer.send(record,callback)
        }.toList
    }
*/






  }
}


class KafkaSink(topic: String,servers: String) extends ForeachWriter[(String,Int,String)]{
  val kafkProperties = new Properties()
  kafkProperties.put("bootstrap.servers",servers)
  kafkProperties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
  kafkProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  val results = new scala.collection.mutable.HashMap[String,String]
  var producer: KafkaProducer[String,String] = _
  def open(partitionId: Long,version:Long): Boolean ={
    producer = new KafkaProducer(kafkProperties)
    true
  }
  def process(value: (String,Int,String)): Unit = {
    producer.send(new ProducerRecord(topic,value._1+":"+value._2+":" +value._3))
  }
  def close(errorOrNull: Throwable): Unit ={
    producer.close()
  }


}
