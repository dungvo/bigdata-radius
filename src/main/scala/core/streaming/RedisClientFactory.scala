package core.streaming

import com.redis.RedisClientPool
import org.apache.log4j.Logger

import scala.collection.mutable

/**
  * Created by hungdv on 28/08/2017.
  */
object RedisClientFactory {
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
    })
  }
}