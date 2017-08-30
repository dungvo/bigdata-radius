package test

import org.apache.spark.sql.SparkSession
import com.redis._
import core.streaming.RedisClientFactory
import org.apache.log4j.{Level, Logger}

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
