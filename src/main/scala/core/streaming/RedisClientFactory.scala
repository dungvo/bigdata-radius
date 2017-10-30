package core.streaming

import java.util

import scala.collection.JavaConverters._
import com.redis.RedisClientPool
import org.apache.log4j.Logger

import scala.collection.mutable
import java.util.HashSet

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}

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

object RedisClusterClientFactory {
  private val logger = Logger.getLogger(getClass)
  private val clients = mutable.HashMap.empty[util.HashSet[HostAndPort],JedisCluster]
  private val MAX_CONNECTIONS_POOL = 60
  private val MAX_WAIT_MILLIS = 2000
  private val MAX_CONNECTION_TIMEOUT = 2000
  private val MAX_SO_TIMEOUT = 2000
  private val MAX_ATTEMP = 5
  val config = new GenericObjectPoolConfig()
    config.setMaxTotal(MAX_CONNECTIONS_POOL)
    config.setMaxWaitMillis(MAX_WAIT_MILLIS)

  /**
    *
    * @param nodes
    * @param poolingConfig
    * @return
    */
  def getOrCreateClient(nodes: util.HashSet[HostAndPort],poolingConfig: GenericObjectPoolConfig): JedisCluster = {
    clients.getOrElseUpdate(nodes,{
      logger.info(s"Create Redis Connection Pool , config: ${nodes.toString}")
      val cluster = new JedisCluster(nodes,MAX_CONNECTION_TIMEOUT,MAX_SO_TIMEOUT,MAX_ATTEMP,poolingConfig)
      clients(nodes) = cluster
      sys.addShutdownHook{
        logger.info(s"Close redis cluster client, nodes : ${nodes.toString}")
        cluster.close()
      }
      val cNodes = cluster.getClusterNodes()
      val keys: util.Set[String] = cNodes.keySet()
      for(key <- keys.asScala){
        logger.info(s"Node : {}  $key")
        val jedis = cNodes.get(key).getResource()
        logger.debug(s"Server info : {}  ${jedis.info()} ")
      }
      cluster
    })
  }

  /**
    * Default pooling config
    * @param nodes
    * @return
    */
  def getOrCreateClient(nodes: util.HashSet[HostAndPort]) : JedisCluster= {
    this.getOrCreateClient(nodes,this.config)
  }
  /**
    * Get client with nodes input string
    * @param nodes nodes in format : "172.27.11.75:6379,172.27.11.80:6379 ......"
    * @return
    */
  def getOrCreateClient(nodes: String): JedisCluster = {
    this.getOrCreateClient(getNodes(nodes),this.config)
  }

  /**
    *
    * @param nodes
    * @param config
    * @return
    */
  def getOrCreateClient(nodes:String, config:GenericObjectPoolConfig ): JedisCluster ={
    this.getOrCreateClient(getNodes(nodes),config)
  }

  /**
    *
    * @param redisClusterString
    * @return
    */
  private def getNodes(redisClusterString: String): java.util.HashSet[HostAndPort] ={
    if (redisClusterString == null || redisClusterString.isEmpty()) {
      logger.error("Redis cluster hosts are not set")
      return null
    }
    val jedisClusterNodes = new util.LinkedHashSet[HostAndPort]()
    val arr: Array[String] = redisClusterString.split(",")
    val pattern = "\\d+".r
    arr.foreach{host =>

      val readHostParts = host.split(":")
      if(readHostParts.length != 2 /*|| ! (readHostParts match {case pattern(c) => true case _ => false })*/){
        logger.error(s"Invalid host name set for redis cluster: {} $host")
      }else{
        logger.info("Adding host {}:{}", readHostParts(0), readHostParts(1));
        jedisClusterNodes.add(new HostAndPort(readHostParts(0), Integer.parseInt(readHostParts(1))));

      }
    }
    jedisClusterNodes
  }




}