package com.ftel.bigdata.radius.classify

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Driver {

  def main(args: Array[String]) {
//    val path = "/data/radius/isc-radius-2017-12-01"
    val path = "radius-log-sample.csv"
    val sc = getSparkContextLocal()
    
    val lines = sc.textFile(path, 1)
    val logs = lines.map(x => Parser(x))
    logs.map(x => x.get() -> 1).reduceByKey(_+_).foreach(println)
    
    /**
     * PATH: "radius-log-sample.csv"
     * (ConLog-Reject,28)
     * (LoadLog,60)
     * (ErrLog,2)
     * (ConLog-LogOff,22)
     * (ConLog-SignIn,9)
     */
    //logs.map(x => )
    //println(loadLogs.count())
    //lines.foreach(println)

    //"ACTALIVE","Dec 01 2017 06:59:59","LDG-MP01-2","796176075","Lddsl-161001-360","1905765","477268962","3712614232","0","1011598","100.91.231.187","64:d9:54:82:37:e4","","1","35","0","0","0","0"

  }

  private def getSparkContext(): SparkContext = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    sc
  }
  
  private def getSparkContextLocal(): SparkContext = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("TEST")
    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    sc
  }
}