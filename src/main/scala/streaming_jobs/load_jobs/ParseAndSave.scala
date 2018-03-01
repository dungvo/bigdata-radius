package streaming_jobs.load_jobs

import java.util

import storage.es.ElasticSearchDStreamWriter._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import parser.{LoadLogLineObject, LoadLogParser}
import core.streaming.{LoadLogBroadcast, RedisClusterClientFactory}
import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import redis.clients.jedis.{HostAndPort, JedisCluster}





/**
  * Created by hungdv on 09/05/2017.
  */
object ParseAndSave {
  val START_POS = 0
  val END_POS = 30
  // Kept length of 10 element in list

  def parserAndSave(ssc: StreamingContext,
                    ss: SparkSession,
                    lines: DStream[String],
                    loadLogParser: LoadLogParser,
                    redisNodes: String
                   ):Unit ={

    val sc = ss.sparkContext
    val bLoadLogParser = LoadLogBroadcast.getInstance(sc,loadLogParser)
    val objectLoadLogs: DStream[LoadLogLineObject] = lines.transform(extractValue(bLoadLogParser)).cache()
    // Save to ES.
    /*try{
      objectLoadLogs.persistToStorageDaily(Predef.Map[String,String]("indexPrefix" -> "radius_load","type" -> "loadLog"))
    }catch {
      case e: Exception => System.err.println("Uncatched Exception occur when save load log to ES : " +  e.getMessage)
      case _ => println("Ignore !")
    }*/
    // Extract SignIn IP - persist to Redis.
    val loadFilterdActAlive = objectLoadLogs.filter(x => x.actStatus == "ACTALIVE")
    //redisNodes: "172.27.11.173:6379,172.27.11.175:6379,172.27.11.176:6379,172.27.11.173:6380,172.27.11.175:6380,172.27.11.176:6380"
    loadFilterdActAlive.foreachRDD{rdd =>
      rdd.foreachPartition{part =>
        val redisClient = RedisClusterClientFactory.getOrCreateClient(redisNodes)

        val kvs: Iterator[(String, String)] = part.map{ logLine =>
          //println(logLine.IPAdress + logLine.name + logLine.date)
          (logLine.IPAdress,logLine.name +","+ DateTime.parse(logLine.date,
                                                  DateTimeFormat.forPattern("MMM dd yyyy HH:mm:ss")).getMillis.toString())}

        kvs.foreach{kv =>
          try{
            //println(kv._1 + "-" + kv._2)
            redisClient.lpush(kv._1,kv._2.toString)
            redisClient.ltrim(kv._1,START_POS,END_POS)
          }catch{
            case e: Exception => println("just simply ignore" + kv._1 + " ---- " + kv._2)
          }
          //should put it on to a transaction though, but cluster does not support trans
        }
      }
    }

    //DEBUG
    /*lines.foreachRDD{

      /*rdd =>
        val x = rdd.count()
        val rddCountMap = Map("count" -> x)
        val sparkContext = rdd.sparkContext
        import org.elasticsearch.spark._
        sparkContext.makeRDD(Seq(rddCountMap)).saveToEs("rdd_count3/test")*/

      rdd =>
        val s = rdd.take(1)
        val map = Map("value" -> s)
        val sparkContext = rdd.sparkContext
        import org.elasticsearch.spark._
        sparkContext.makeRDD(Seq(map)).saveToEs("rdd_string2/test")
    }*/


    //objectLoadLogs.sa
  }


  def extractValue = (parser:Broadcast[LoadLogParser]) => (lines: RDD[String]) =>
      lines.map({line =>
        val parsedObject = parser.value.extractValues(line).getOrElse(None)
        parsedObject match{
          case Some(x) => x.asInstanceOf[LoadLogLineObject]
          case _ => None
        }
        parsedObject
      }).filter(x => x != None).map(ob => ob.asInstanceOf[LoadLogLineObject])

}
