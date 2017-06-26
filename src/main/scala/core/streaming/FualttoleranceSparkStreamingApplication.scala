package core.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{StreamingContext,Seconds}
import core.streaming.SparkStreamingApplication
import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 03/05/2017.
  */
/*
 If the directory ~/checkpoint/ does not exist (e.g. running for the first time), it will create
 * a new StreamingContext (will print "Creating new context" to the console). Otherwise, if
 * checkpoint data exists in ~/checkpoint/, then it will create StreamingContext from
 * the checkpoint data.
 *
 * Refer to the online documentation for more details.
 * https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/RecoverableNetworkWordCount.scala
 * http://spark.apache.org/docs/latest/streaming-programming-guide.html#caching--persistence
 */
trait FualttoleranceSparkStreamingApplication extends SparkApplication{
  def streamingBatchDuration: FiniteDuration

  def streamingCheckpointDir: String
  //NEw API - Spark Session. - not tested yet
  def withSparkStreamingContext(f: (SparkSession,StreamingContext)=> Unit): Unit = {
    withSparkSession{
      sparkSession =>

        val streamingContext = StreamingContext.getOrCreate(streamingCheckpointDir,
           () => createStreamingContext(sparkSession))

        f(sparkSession,streamingContext)

        streamingContext.start()
        streamingContext.awaitTermination()
    }
  }

  def createStreamingContext(sparkSession : SparkSession): StreamingContext ={
    val sparkContext = sparkSession.sparkContext

    val streamingContext = new StreamingContext(sparkContext,Seconds(streamingBatchDuration.toSeconds))
    streamingContext.checkpoint(streamingCheckpointDir)
    streamingContext
  }

}
