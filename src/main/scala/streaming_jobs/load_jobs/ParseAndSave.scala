package streaming_jobs.load_jobs

import storage.es.ElasticSearchDStreamWriter._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import parser.{LoadLogLineObject, LoadLogParser}
import core.streaming.LoadLogBroadcast
import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream._





/**
  * Created by hungdv on 09/05/2017.
  */
object ParseAndSave {

  def parserAndSave(ssc: StreamingContext,
                    ss: SparkSession,
                    lines: DStream[String],
                    loadLogParser: LoadLogParser
                   ):Unit ={

    val sc = ss.sparkContext
    val bLoadLogParser = LoadLogBroadcast.getInstance(sc,loadLogParser)
    val objectLoadLogs: DStream[LoadLogLineObject] = lines.transform(extractValue(bLoadLogParser)).cache()
    // Save to kafka
    // Extract SignIn IP - persist to Redis.
    objectLoadLogs.foreachRDD{rdd =>
      rdd.foreachPartition{part =>

      }
    }


    // Save to ES.
    try{
      objectLoadLogs.persistToStorageDaily(Predef.Map[String,String]("indexPrefix" -> "radius_load","type" -> "loadLog"))
    }catch {
      case e: Exception => System.err.println("Uncatched Exception occur when save load log to ES : " +  e.getMessage)
      case _ => println("Ignore !")
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
