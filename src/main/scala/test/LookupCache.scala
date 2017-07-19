package test

import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

/**
  * Created by hungdv on 19/07/2017.
  */
object LookupCache {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("Test").master("local[2]").getOrCreate()
    val sc = sparkSession.sparkContext
    import org.apache.spark.streaming.{ StreamingContext, Seconds }
    val ssc = new StreamingContext(sc, batchDuration = Seconds(5))

    // checkpointing is mandatory
    ssc.checkpoint("/tmp")

    val rdd = sc.parallelize(0 to 9).map(n => (n, n % 2 toString))
    import org.apache.spark.streaming.dstream.ConstantInputDStream
    val sessions = new ConstantInputDStream(ssc, rdd)

    import org.apache.spark.streaming.{State, StateSpec, Time}
    val updateState = (batchTime: Time, key: Int, value: Option[String], state: State[Int]) => {
      println(s">>> batchTime = $batchTime")
      println(s">>> key = $key")
      println(s">>> value = $value")
      println(s">>> state = $state")
      val sum = value.getOrElse("").size + state.getOption.getOrElse(0)
      state.update(sum)
      Some((key, value, sum)) // mapped value
    }
    val spec = StateSpec.function(updateState)
    val mappedStatefulStream = sessions.mapWithState(spec)

    mappedStatefulStream.print()
  }

}
