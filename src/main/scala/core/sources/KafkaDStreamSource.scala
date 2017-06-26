package core.sources
import core.KafkaPayLoad
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
/**
  * Created by hungdv on 10/03/2017.
  */
class KafkaDStreamSource(configs: Map[String,Object]) extends Serializable{

 /* def createSource(scc: StreamingContext,topic: String): DStream[KafkaPayLoad] ={
    val kafkaParams = configs
    val kafkaTopics = Set(topic)

    KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
      scc,
      PreferConsistent, //locationStrategy
      Subscribe[Array[Byte], Array[Byte]](kafkaTopics,kafkaParams) //consumerStrategy
    ).map(dstream => KafkaPayLoad(Option(dstream.key),dstream.value))
  }*/
  /**
    * Kafka message 's value Only
    * @param scc
    * @param topic
    * @return Create Dstream from Kafka source, return Dstreamp[KafkaMessage.value]
    */
   def createSource(scc: StreamingContext,topic: String): DStream[String] ={
   val kafkaParams = configs
   val kafkaTopics = Set(topic)

   KafkaUtils.createDirectStream[String,String](
     scc,
     PreferConsistent, //locationStrategy
     Subscribe[String, String](kafkaTopics,kafkaParams) //consumerStrategy
   ).map({dstream =>
     //println("Partition : " + dstream.partition() +" : " + dstream.offset())
     dstream.value
   })

 }

  /**
    * Kafka Payload - key and value
    * @param scc
    * @param topic
    * @return Create Dstream from Kafka source, return Dstreamp[(KafkaMessage.key,KafkaMessage.value)]
    */
  def createSourceOfKafkaPayload(scc: StreamingContext,topic: String): DStream[KafkaPayLoad] ={
    val kafkaParams = configs
    val kafkaTopics = Set(topic)

    KafkaUtils.createDirectStream[String,String](
      scc,
      PreferConsistent, //locationStrategy
      Subscribe[String, String](kafkaTopics,kafkaParams) //consumerStrategy
    ).map(dstream => KafkaPayLoad(Option(dstream.key),dstream.value))
  }

  /**
    * Timestamp (moment when msg written to kafka) and value.
    * @param scc
    * @param topic
    * @return
    */
  def createSourceWithTimeStamp(scc: StreamingContext,topic: String): DStream[String] ={
    val kafkaParams = configs
    val kafkaTopics = Set(topic)

    KafkaUtils.createDirectStream[String,String](
      scc,
      PreferConsistent, //locationStrategy
      Subscribe[String, String](kafkaTopics,kafkaParams) //consumerStrategy
    ).map({dstream =>
      //println("Partition : " + dstream.partition() +" : " + dstream.offset())
      // Append moment when msg recvieved .
      dstream.value  + "-" + dstream.timestamp
      // Stable
      //dstream.value()
    })

  }
}

object KafkaDStreamSource{
  def apply(configs: Map[String, String]): KafkaDStreamSource = new KafkaDStreamSource(configs)
}
 