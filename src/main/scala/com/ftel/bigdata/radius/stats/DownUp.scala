package com.ftel.bigdata.radius.stats

import org.apache.spark.rdd.RDD

case class DownUp(name: String, download: Long, upload: Long) {
  def this(array: Array[String]) = this(array(0), array(1).toLong, array(2).toLong)
  override def toString = Array(name, download, upload).mkString("\t")
  def toPair = name -> (download, upload)
}

object DownUp {
  
  def save(rdd: RDD[LoadStats], day: String) {
    // download/upload by day
    rdd.filter(x => x.download >= 0 || x.upload >= 0)
       .map(x => (x.name.toLowerCase()) -> (x.downloadUsage, x.uploadUsage))
       .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
       .map(x => DownUp(x._1, x._2._1, x._2._2))
       .coalesce(32, false, None)
       .saveAsTextFile(s"/data/radius/stats/${day}/download-upload")
  }
}