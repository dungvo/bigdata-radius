package util

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._
import scala.collection.{Map, mutable}
/**
  * Created by hungdv on 02/03/2017.
  */
object SparkCommon {
  val colBicycle  = "bike_aggregation"
  val mongoUrl    = "mongodb://localhost:27017/streamdb." + colBicycle
  lazy val  conf= new SparkConf()
  conf.set("spark.mongodb.input.uri", mongoUrl)
    .set("spark.mongodb.output.uri", mongoUrl)
  def convertRDDToConcurrentHashMap[K,V](rdd: RDD[(K,V)]): mutable.Map[K,V] = {
    val map = new ConcurrentHashMap[K,V]() asScala
    val data = rdd.collect()
    map.sizeHint(data.length)
    data.foreach({pair => map.put(pair._1,pair._2)})
    map
  }
  // Actually collectAsMap return HashMap
  def convertRDDToMap[K,V](rdd: RDD[(K,V)]): Map[K, V] ={
    val map  = new mutable.HashMap[K,V]
    val data = rdd.collect()
    map.sizeHint(data.length)
    data.foreach({pair => map.put(pair._1,pair._2)})
    map
  }
}
