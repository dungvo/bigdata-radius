package test

import java.util

import org.apache.spark.sql.SparkSession
import com.redis._
import core.streaming.{RedisClientFactory, RedisClusterClientFactory}
import org.apache.log4j.{Level, Logger}
import java.util.HashSet

import com.redis.cluster.{ClusterNode, KeyTag, RedisCluster, RegexKeyTag}
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}

import scala.collection.mutable

/**
  * Created by hungdv on 28/08/2017.
  */
object RedisClientTest {
  def main(args: Array[String]): Unit = {
    //val ss = SparkSession.builder().appName("RedisTest").master("local[2]").getOrCreate()
/*    val r = new RedisClient("172.27.11.141", 6373)
    r.set("key","some value")
    val result = r.get("key")
    println(result)*/


    //val clients = RedisClientFactory.getOrCreateClient(("172.27.11.141", 6373))



    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().appName("Test agg string").master("local").getOrCreate()
    val seq = Seq((1,"foo"),(1,"bar"),(1,"far"),(1,"far"),(2,"boo"),(2,"foa"))
    val rdd = sparkSession.sparkContext.parallelize(seq)
    val logOff = rdd.groupByKey().map(x => (x._1, x._2.toSet.toList))

   logOff.foreachPartition{
     partition =>
       partition.foreach{tuple =>
         val clients = RedisClientFactory.getOrCreateClient(("172.27.11.141", 6373))
         clients.withClient{client =>
           tuple._2.foreach(client.lpush(tuple._1,_))
           //client.lpush(tuple._1,tuple._2)
           client.expire(tuple._1,720)
         }
       }

   }







  }

}



object RedisClientTest2 {
  def main(args: Array[String]): Unit = {
    //val ss = SparkSession.builder().appName("RedisTest").master("local[2]").getOrCreate()
       val r = new RedisClient("172.27.11.141", 6373)
        //r.set("key","some value")

        //val result = r.lrange("HNI-MP-01-02-2017-08-28 15:45",0,-1)
        //println(result.get.flatten)
        val keys = r.scan(0, "*", 100)
        keys.foreach(println(_))


    //val clients = RedisClientFactory.getOrCreateClient(("172.27.11.141", 6373))


  }

}


object JedisClusterTest{
  def main(args: Array[String]): Unit = {
    val jedisClusterNodes = new util.HashSet[HostAndPort]()
    jedisClusterNodes.add(new HostAndPort("172.27.11.173",6379))
    jedisClusterNodes.add(new HostAndPort("172.27.11.175",6379))
    jedisClusterNodes.add(new HostAndPort("172.27.11.176",6379))
    val cluster = new JedisCluster(jedisClusterNodes)
    cluster.set("foo2","baa")
    val value = cluster.get("foo2")
    println(value)
    // Cause Erro
    //val host1 = new Jedis("172.27.11.173",6379)
    //println(host1.get("foo"))
    // Cuase Erro
    //val host2 = new Jedis("172.27.11.175",6379)
    //println(host2.get("foo"))

    val nodes = Array(ClusterNode("node1","172.27.11.173",6379),
      ClusterNode("node2", "172.27.11.175", 6379),
      ClusterNode("node3", "172.27.11.176",6379)
    //,
/*      ClusterNode("node4", "172.27.11.173",6380),
      ClusterNode("node5", "172.27.11.175",6380),
      ClusterNode("node6", "172.27.11.176",6380)*/
    )
    val r = new RedisCluster(new mutable.WrappedArray.ofRef(nodes): _*) {
      override val keyTag: Option[KeyTag] = Some(RegexKeyTag)
    }
    print("old redis client : " +r.get("foo2"))
  }
}


object RedisClusterFactoryTest{
  def main(args: Array[String]): Unit = {
    val jedisClusterNodes = new util.HashSet[HostAndPort]()
    jedisClusterNodes.add(new HostAndPort("172.27.11.173",6379))
    jedisClusterNodes.add(new HostAndPort("172.27.11.175",6379))
    jedisClusterNodes.add(new HostAndPort("172.27.11.176",6379))
    jedisClusterNodes.add(new HostAndPort("172.27.11.176",6380))
    jedisClusterNodes.add(new HostAndPort("172.27.11.176",6380))
    jedisClusterNodes.add(new HostAndPort("172.27.11.176",6380))



    val cluster2 = RedisClusterClientFactory.getOrCreateClient(jedisClusterNodes)
    val cluster = RedisClusterClientFactory.getOrCreateClient("172.27.11.173:6379,172.27.11.175:6379,172.27.11.176:6379,172.27.11.173:6380,172.27.11.175:6380,172.27.11.176:6380")
    for(a <- 0 to 10){
      try{
        cluster.set("key",a.toString)
        println(cluster.get("key"))
      }catch{
        case e: Exception => println("There is an error")
      }

    }

  }
}
