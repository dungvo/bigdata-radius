package core

/**
  * Created by hungdv on 10/03/2017.
  */
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import scala.collection.mutable
import org.apache.kafka.common.serialization.{ByteArraySerializer,ByteArrayDeserializer}
import com.redis._
object KafkaProducerFactory {
  //VERY IMPORTANT import
  import scala.collection.JavaConversions._
  private val logger = Logger.getLogger(getClass)

  private val producers = mutable.HashMap.empty[Map[String,Object], KafkaProducer[_, _]]

  def getOrCreateProducer[K,V](config: Map[String,Object]): KafkaProducer[K, V] = {

    //Should remove this config
    val defaulConfig = Map(
     /* "key.serializer" -> classOf[ByteArraySerializer],
      "value.serializer" -> classOf[ByteArraySerializer]*/
      //"key.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      //"value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer"
    )

    val finalConfig = defaulConfig ++ config


    producers.getOrElseUpdate(finalConfig,{
      logger.info(s"Create Kafka producer , config: $finalConfig")
      val producer = new KafkaProducer[K,V](finalConfig)
      producers(finalConfig) = producer
      sys.addShutdownHook{
        logger.info(s"Close Kafka producer, config: $finalConfig")
        producer.close()
      }
      producer
    }).asInstanceOf[KafkaProducer[K,V]]
  }
}


object RedisClientFactory{
  import scala.collection.JavaConversions._
  private val logger = Logger.getLogger(getClass)

  private val producers = mutable.HashMap.empty[(String,Int), RedisClientPool]

  def getOrCreateClient(config: (String,Int)): RedisClientPool = {

    //Should remove this config


    producers.getOrElseUpdate(config,{
      logger.info(s"Create Redis Connection Pool , config: $config")
      val producer = new RedisClientPool(config._1,config._2)
      producers(config) = producer
      sys.addShutdownHook{
        logger.info(s"Close Redis Connection Pool , config: $config")
        producer.close
      }
      producer
    }).asInstanceOf[RedisClientPool]
  }
}