package streaming_jobs.dns

import java.util.UUID

import core.KafkaProducerFactory
import core.sinks.KafkaDStreamSinkExceptionHandler
import core.streaming.{DNSParsergBroadcast, RedisClusterClientFactory}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.exceptions.JedisDataException

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
/**
  * Created by hungdv on 31/10/2017.
  */
object ParseAndMap {
  def parserAndSave(ssc: StreamingContext,
                    ss: SparkSession,
                    lines: DStream[String],
                    parser: DNSParser,
                    redisNodes: String,
                    kafkaProducerConfig: Predef.Map[String,String],
                    kafkaOutputTopic: String
                   ):Unit ={
    val sc = ss.sparkContext
    val bDNSParser = DNSParsergBroadcast.getInstance(sc,parser)
    val bProducerConfig = sc.broadcast[Map[String,String]](kafkaProducerConfig)
    val bKafkaDNSMapingOutput = sc.broadcast[String](kafkaOutputTopic)
    val dnsObject: DStream[(Long,String, String)]  = lines.transform(extractValue(bDNSParser)).cache()
    dnsObject.foreachRDD{rdd =>
      rdd.foreachPartition{part =>
        if(part.hasNext){
          val redisClient = RedisClusterClientFactory.getOrCreateClient(redisNodes)
          val producer: KafkaProducer[String,String] = KafkaProducerFactory.getOrCreateProducer(bProducerConfig.value)
          val context = TaskContext.get()
          val callback = new KafkaDStreamSinkExceptionHandler
          //Test-Debugs
          /*try{
            val result = part.map{ x =>

              try{
                val candidates = redisClient.lrange(x._2,0,-1).toList
              }catch {
                case e : JedisDataException => println("+JedisDataException : Key  : " + x._2)
                case _ => println("+Ignored")
              }
              val candidates = redisClient.lrange(x._2,0,-1).toList
              val name = RedisHelper.mapping(candidates,x._1)
              val string = name + " " + x._3
              val record = new ProducerRecord[String,String](bKafkaDNSMapingOutput.value,UUID.randomUUID().toString,string)
              callback.throwExceptionIfAny()
              producer.send(record,callback)
            }.toList
          }catch{
            case e: JedisDataException => println("-JedisDataException : In part : " )
            case e: SparkException => println("-SparkException")
            case _ => println("-Ignored")
          }
*/
          val result = part.map{ x =>

            val candidates = redisClient.lrange(x._2,0,-1).toList
            val name = RedisHelper.mapping(candidates,x._1)
            // # Name + IP + Time + Domain
            val string = name + " "  + x._2 + " " + x._1  + " " + x._3
            val record = new ProducerRecord[String,String](bKafkaDNSMapingOutput.value,UUID.randomUUID().toString,string)
            callback.throwExceptionIfAny()
            producer.send(record,callback)
          }.toList
        }
      }
    }

  }
  def extractValue = (parser: Broadcast[DNSParser]) => (lines: RDD[String]) =>
    lines.map{line =>
      val parsedObject = parser.value.parse(line)
      parsedObject
    }.filter(x => x._1 != -1)
}
