package test

import java.util

import org.apache.spark.sql.SparkSession
import com.redis._
import core.streaming.{RedisClientFactory, RedisClusterClientFactory}
import org.apache.log4j.{Level, Logger}
import java.util.HashSet

import com.redis.cluster.{ClusterNode, KeyTag, RedisCluster, RegexKeyTag}
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.io.Path
import scala.tools.nsc.classpath.FileUtils.FileOps


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
    //Good
    val r = new Jedis("172.27.11.173", 6380)
    //Bad
    val r1 = new Jedis("172.27.11.173", 6379)
    val r2 = new Jedis("172.27.11.175", 6379)
    val r3 = new Jedis("172.27.11.176", 6379)
    val r4 = new Jedis("172.27.11.175", 6380)
    val r5 = new Jedis("172.27.11.176", 6380)


       //val r = new RedisClient("172.27.11.173", 6379)
        //r.set("key","some value")

        //val result = r.lrange("HNI-MP-01-02-2017-08-28 15:45",0,-1)
        //println(result.get.flatten)
        //val keys = r.scan(0, "*", 100)
        //keys.foreach(println(_))
        //val test = r.get("100.116.52.147")
    //val test1 = r.get("100.116.52.147")
    //val test2 = r2.get("100.116.52.147")
    //val test3 = r3.get("100.116.52.147")
    //val test4 = r4.get("100.116.52.147")
    //val test5 = r5.get("100.116.52.147")
    val r2Keys = r2.keys("*")
    println("length r2 : " + r2Keys.size())
    val r3keys = r3.keys("*")
    println("length r3 : " + r3keys.size())
    val r4keys = r4.keys("*")
    println("length r4 : " + r4keys.size())
    val r5keys = r5.keys("*")
    println("length r5 : " + r5keys.size())
    val rkeys = r.keys("*")
    println("length r : " + rkeys.size())
    val r1keys = r1.keys("*")
    println("length r1 : " + r1keys.size())



    //val clients = RedisClientFactory.getOrCreateClient(("172.27.11.141", 6373))


  }

}


object RedisKeysCleanup {
  def main(args: Array[String]): Unit = {
    //val ss = SparkSession.builder().appName("RedisTest").master("local[2]").getOrCreate()
    //Good
    val r = new Jedis("172.27.11.173", 6380)
    //Bad
    val r1 = new Jedis("172.27.11.173", 6379)
    val r2 = new Jedis("172.27.11.175", 6379)
    //val r3 = new Jedis("172.27.11.176", 6379)
    //val r4 = new Jedis("172.27.11.175", 6380)
    //val r5 = new Jedis("172.27.11.176", 6380)


    //val r = new RedisClient("172.27.11.173", 6379)
    //r.set("key","some value")

    //val result = r.lrange("HNI-MP-01-02-2017-08-28 15:45",0,-1)
    //println(result.get.flatten)
    //val keys = r.scan(0, "*", 100)
    //keys.foreach(println(_))
    //val test = r.get("100.116.52.147")
    //val test1 = r.get("100.116.52.147")
    //val test2 = r2.get("100.116.52.147")
    //val test3 = r3.get("100.116.52.147")
    //val test4 = r4.get("100.116.52.147")
    //val test5 = r5.get("100.116.52.147")

    //cleanup
    //clean(r)
    //clean(r1)
    //clean(r2)
    //val path1 = "/home/hungdv/dns_ip/key1.txt"
    //val path2 = "/home/hungdv/dns_ip/key2.txt"
    //val path3 = "/home/hungdv/dns_ip/key3.txt"
    //exportKeys(r,path1)
    //exportKeys(r1,path2)
    //exportKeys(r2,path3)

    val key = r.keys("*").toSet
    val key1 = r1.keys("*").toSet
    val key2 = r2.keys("*").toSet
    val keys = key.union(key1).union(key2)
    println(keys.size)
    val list = List("202.58.98.218","112.72.96.138","210.245.24.83","210.245.24.66","100.94.123.80","42.118.246.238")
    list.foreach{
      x => check(keys,x)
    }

    //val clients = RedisClientFactory.getOrCreateClient(("172.27.11.141", 6373))


  }
  def check(keys: Set[String],key: String) = {
    if(keys.contains(key)) println("key: " + key + " is in cache")
    else println("key: " + key + " is NOT in cache")
  }
  def clean(r: Jedis): Unit ={
    var count = 0
    val rKeys = r.keys("*").iterator()
    for(key <- rKeys){
      if(r.`type`(key) == "string"){
        val value = r.get(key)
        r.del(key)
        r.lpush(key,value)
        count = count + 1
      }
    }
    println(" number of error : " + count)
  }
  def exportKeys(r: Jedis,path: String): Unit= {
    var count = 0
    val rKeys: util.Iterator[String] = r.keys("*").iterator()
    import java.io._
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path)))

    rKeys.foreach{ x=>
      writer.write(x + "\n")
      count = count +1
    }
    writer.close()
    println("write : " + count + " keys")
  }
  def getDistinctKeySet(){

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
    val value2: Option[String] = r.get("foo3")
    print("old redis client : " + value2)
    if(value2.isEmpty){
      println("true")
    }else{

    }
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
    cluster.set("11","11")
    val value = cluster.get("12")
    if(value == null){println("true")}
    println(value)
    for(a <- 0 to 10){
      try{
        cluster.set("key",a.toString)

        //println(cluster.get("key"))
        //cluster.lpush("list",a.toString)
        //cluster.ltrim("list",0,9)
      }catch{
        case e: Exception => println("There is an error")
      }

    }
    //println(cluster.lrange("list",0,-1))
    //cluster.lpush("100.98.144.72","1509411600000")
    //cluster.lpop("100.98.144.72")
    //println(cluster.lrange("100.116.52.147",0,9))
    //println(cluster.get("100.116.52.147"))

    println(cluster.`type`("118.70.200.214"))
    println(cluster.`type`("100.99.11.205"))
    println(cluster.`type`("210.245.24.100"))
    println(cluster.`type`("100.116.52.147"))
    //cluster.lpush("100.116.52.147","tidsl-130727-565,1509426091000")

    val keys = cluster

  }
}
