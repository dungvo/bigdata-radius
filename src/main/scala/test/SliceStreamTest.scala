package test

/**
  * Created by hungdv on 13/06/2017.
  */
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

import scala.collection.mutable.Queue

object SliceStreamTest {
  /**
    * This script demonstrates how to leverage DStream.slice to "drain"
    * a DStream every now and then.
    */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
    val sc = sparkSession.sparkContext

    val batchInterval = Seconds(1)
   val ssc = new StreamingContext(sc, batchInterval)

  //
  // Generate a queue of 1000 input RDDs to form a DStream
  //
  val qSize = 100
  def inRdd = sc.parallelize(0 until 1000000)
  val q = Queue(inRdd)
  (1 until qSize).foreach(_ => q += inRdd)
  val ds = ssc.queueStream(q, oneAtATime = true)
  ds.foreachRDD(_ => {})

  //
  // 'fromTime' represents the start of the slice interval.
  // It is initialized before calling StreamingContext.start() to cover the
  // 'zeroTime' of the DStream, which is initialized during the
  // StreamingContext.start() call.
  //
  val startTime = System.currentTimeMillis
  var fromTime = startTime

  ssc.start

  val rand = scala.util.Random
  var totalRetrieved = 0
  try {
    while (ssc.getState == StreamingContextState.ACTIVE && totalRetrieved < qSize) {
      //
      // Drain the DStream up to current time
      //
      val toTime = System.currentTimeMillis
      val ti = Time(toTime)
      val from  = ti.floor(batchInterval)

      val sliceRDDs: Seq[RDD[Int]] = ds.slice(from, from +  batchInterval)
      val rdds = sliceRDDs.reduceLeft(_ union _)
      rdds.foreach(println(_))
      //val sliceRDDs = ds.slice(Time(fromTime), Time(toTime))
      //
      // The next fromTime is a batchInterval apart from the current 'toTime'.
      //
      // This is because a DStream is discretized.  RDDs in the DStream are each
      // uniquely identified by their timestamps, which are a batchInterval apart
      // of each other:
      //   RDD @ zeroTime
      //   RDD @ (zeroTime + batchInterval)
      //   RDD @ (zeroTime + batchInterval * 2)
      //   ...
      //
      // The 'fromTime'  and 'toTime' here may not fall exactly at a discretized
      // timestamp.  What the DStream.slice() function does is to map them to
      // their next discretized timestamps.
      //
      //
      fromTime = toTime + batchInterval.milliseconds

      //
      // Log number of retrieved RDDs so far, and compare it with the estimated
      // number of RDDs that are already in the DStream.
      //
      totalRetrieved += sliceRDDs.size
      println(s"Retrieved ${sliceRDDs.size} RDDs (total ${totalRetrieved}) at ${toTime-startTime}ms (${(toTime-startTime)/batchInterval.milliseconds} batches) since start time.")

      //
      // Sleep for a random amount of time up to 1.5 * the batch interval.
      // This is to emulate some operation that is longer than the batch interval,
      // creating a situation where more than one RDD will be resulted in some
      // slice, for demonstration.
      //
      val sleepMax = batchInterval.milliseconds + (batchInterval.milliseconds/2)
      val sleepTime = rand.nextInt % (sleepMax/2) + sleepMax/2
      //println(s"  sleeping for ${sleepTime} milleseconds...")
      Thread.sleep(sleepTime)
    }
  } finally {
    ssc.stop(false)
  }
  }

}
