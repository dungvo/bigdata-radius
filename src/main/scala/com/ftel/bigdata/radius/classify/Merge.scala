package com.ftel.bigdata.radius.classify

import com.ftel.bigdata.conf.Configure
import org.apache.hadoop.fs.FileSystem
import com.ftel.bigdata.utils.HdfsUtil
import org.apache.spark.SparkContext

object Merge {
  
  def main(args: Array[String]) {
    //run(args(0))
    val day = "2017-12-01"
    val month = day.substring(0, 7)
    println(month)
  }
  
  def run(sc: SparkContext, day: String) {
    //val time0 = System.currentTimeMillis()
    //val sc = Configure.getSparkContext()
    val fs = FileSystem.get(sc.hadoopConfiguration)
    merge(fs, day)
  }


  def merge(fs: FileSystem, day: String) {
    val time0 = System.currentTimeMillis()

    val dir = s"/data/radius/temp/classify/${day}"
    val out = s"/data/radius/classify"

    // List all in Day
    val files = HdfsUtil.ls(fs, dir, true).map(x => x.substring(dir.length())).filter(x => !x.endsWith("_SUCCESS"))
    val outDirs = files.map(x => out + x.substring(0, x.lastIndexOf("/")))
    
    // Create directory
    outDirs.foreach(x => {
      if (HdfsUtil.isExist(fs, x)) {
        //println("The dir " + x + " is exist")
      } else {
        println("Create Dir " + x)
        HdfsUtil.mkdir(fs, x)
      }
    })

    //    
    // Move File
    var processed = 0
    val size = files.size

    files.foreach(x => {
      val src = dir + x
      val dest = out + x.substring(0)
      //println(src + "->" + dest)
      HdfsUtil.mv(fs, src, dest)
      processed = processed + 1
      println(s"Processed: $processed/$size")
    })

    //fs.close()
    val time1 = System.currentTimeMillis()

    println("Time: " + (time1 - time0))
  }
}