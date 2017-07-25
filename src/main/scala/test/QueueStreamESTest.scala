package test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Queue
import scala.concurrent.duration.FiniteDuration

/**
  * Created by hungdv on 11/05/2017.
  */
object QueueStreamESTest{
  def main(args: Array[String]) {
    implicit def finiteDurationToSparkDuration(value: FiniteDuration): Duration = new Duration(value.toMillis)
    val sparkConf = new SparkConf()
    sparkConf.set("es.port","9200")
      .set("es.nodes","localhost")
      .set("es.http.timeout","5m")
      .set("es.scroll.size","50")
      .set("es.index.auto.create", "true")
      .setMaster("local[2]")
      .setAppName("EsTest")


    val ss: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc: SparkContext = ss.sparkContext
    // Create the context
    val ssc = new StreamingContext(sc, Seconds(5))

    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    val rddQueue = new Queue[RDD[Int]]()
    for (i <- 1 to 300) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)
      }
      //Thread.sleep(3000)
    }
    var totalRetrieved = 0
    val qSize = 100
    def inRdd = sc.parallelize(0 until 1000000)
    val q = Queue(inRdd)
    (1 until qSize).foreach(_ => q += inRdd)
    val ds = ssc.queueStream(q, oneAtATime = true)
    ds.foreachRDD(_ => {})

    // Create the QueueInputDStream and use it do some processing
/*    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue)
    inputStream.foreachRDD{_ => {}}*/
    ssc.start()


    try{
      while(ssc.getState() == StreamingContextState.ACTIVE && totalRetrieved < qSize){
        //val mappedStream: DStream[(Int, Int)] = inputStream.map(x => (x % 10, 1))
        val time = new Time(System.currentTimeMillis)
        val slideDruration =  Seconds(15)
        val intervalBegin = time.floor(slideDruration)
        //val sliced: Seq[RDD[(Int, Int)]] = mappedStream.slice(new Time(0),new Time(4))
        //val sliced: Seq[RDD[(Int, Int)]] = mappedStream.slice(intervalBegin,intervalBegin + slideDruration)
        val dss = ds.transform(tf)
        val sliced = dss.slice(intervalBegin,intervalBegin + slideDruration)
        println(s"Retrieved ${sliced.size} RDDs.")

        val rdd = sliced.reduce(_ union _)
        rdd.foreach(println(_))

      }
    }finally {
      ssc.stop()

    }
    //mappedStream.slice(Time(0), Time(System.currentTimeMillis)).map(_.collect().toList)

    /*val time = new Time(System.currentTimeMillis)
    val slideDruration = new Duration(90)
    val intervalBegin = time.floor(slideDruration)
    //val sliced: Seq[RDD[(Int, Int)]] = mappedStream.slice(new Time(0),new Time(4))
    val sliced: Seq[RDD[(Int, Int)]] = mappedStream.slice(intervalBegin,intervalBegin + slideDruration)
    val rdd = sliced.reduce(_ union _)
    rdd.foreach(println(_))*/
/*    val reducedStream: DStream[(Int, Int)] = mappedStream.reduceByKey(_ + _)
    reducedStream.print()
    import storage.es.ElasticSearchDStreamWriter._
    reducedStream.persistToStorage(Map[String,String]("index" -> "spark_es_test_que_slide_stream","type" -> "case_class"))*/


    // Create and push some RDDs into rddQueue


  }
  def tf = (lines: RDD[Int]) => lines.map{
    line =>
      line
  }
}
