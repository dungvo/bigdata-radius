package com.ftel.bigdata.radius.stats

import org.apache.spark.rdd.RDD

case class DownUp(day: String, name: String, download: Long, upload: Long) {
//  def this(array: Array[String]) = this(array(0), array(1), array(2).toLong, array(3).toLong)
  def this(day: String, array: Array[String]) = this(day, array(0), array(1).toLong, array(2).toLong)
  override def toString = Array(name, download, upload).mkString("\t")
  def toPair = name -> (download, upload)
}

object DownUp {
  
  def save(rdd: RDD[LoadStats], day: String) {
    // download/upload by day
    rdd.map(x => (x.name.toLowerCase()) -> (if (x.download > 0) x.download else 0, if (x.upload > 0) x.upload else 0))
       .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
       .map(x => DownUp(day, x._1, x._2._1, x._2._2))
       .coalesce(32, false, None)
       .saveAsTextFile(s"/data/radius/stats/${day}/download-upload")
  }
}