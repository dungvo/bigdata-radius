package test

import akka.event.Logging.LogLevel
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.Queue
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext, Time}
import org.apache.spark.rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.MapWithStateDStream

import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 19/07/2017.
  */
object QueueStream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    implicit def finiteDurationToSparkDuration(value: FiniteDuration): Duration = new Duration(value.toMillis)
    val sparkSession: SparkSession = SparkSession.builder().appName("QueueStreamTest").master("local[2]").getOrCreate()
    val sc = sparkSession.sparkContext
    val ssc = new StreamingContext(sc,Seconds(1))
    ssc.checkpoint("/tmp")
    val rddQueue = new Queue[RDD[Int]]()
    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map{ x =>
      (x," value of x : " + x)
    }
    mappedStream.print()

    import org.apache.spark.streaming.{State, StateSpec, Time}
    val updateState = (batchTime: Time, key: Int, value: Option[String], state: State[Int]) => {
      println(s">>> batchTime = $batchTime")
      println(s">>> key = $key")
      println(s">>> value = $value")
      println(s">>> state = $state")
      val sum: Int = value.getOrElse("").length + state.getOption.getOrElse(0)
      state.update(sum)
      Some((key, value, sum)) // mapped value
    }

    val currentState = (batchTime: Time,key: Int,value: Option[String],state: State[String]) => {
      val newState = value.getOrElse("n/a - ") + batchTime.toString()
      state.update(newState)
      Some(key,value,newState)
    }



    //val spec = StateSpec.function(updateState)
    val spec2 = StateSpec.function(currentState)
    //val mappedStatefulStream: MapWithStateDStream[Int, String, Int, (Int, Option[String], Int)] = mappedStream.mapWithState(spec)
    val mapped2 = mappedStream.mapWithState(spec2)
    mapped2.print()

    //mappedStatefulStream.foreachRDD{rdd => rdd.foreach(println(_))}



    ssc.start()

    for(i <- 1 to 3000){
      rddQueue.synchronized{
        rddQueue += ssc.sparkContext.makeRDD(1 to 100,10)
        Thread.sleep(500)
      }
    }
    ssc.stop()
  }
}
