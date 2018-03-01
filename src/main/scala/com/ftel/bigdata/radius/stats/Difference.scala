package com.ftel.bigdata.radius.stats

import com.ftel.bigdata.conf.Configure
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import com.ftel.bigdata.utils.HdfsUtil
import com.ftel.bigdata.utils.DateTimeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class Difference(name: String, download: Long, upload: Long, downloadDiff: Long, uploadDiff: Long) {
  def this(array: Array[String]) = this(array(0), array(1).toLong, array(2).toLong, array(3).toLong, array(4).toLong)
  def this(name: String, downup: (Long, Long), diff: (Long, Long)) = this(
      name, downup._1, downup._2, diff._1, diff._2
      )
  def toPair = name -> (download, upload, downloadDiff, uploadDiff)
  override def toString = Array(name, download, upload, downloadDiff, uploadDiff).mkString("\t")
}

object Difference {
  
  def main(args: Array[String]) {
    val sample1 = Array(
          DownUp("A", 10, 20),
          DownUp("B", 10, 30),
          DownUp("C", 20, 30)
        )
        
    val sample2 = Array(
          DownUp("A", 20, 30),
          DownUp("C", 10, 30),
          DownUp("D", 10, 30)
        )
        
    val sc = Configure.getSparkContextLocal()
    
    val rdd1 = sc.parallelize(sample1, 1)
    val rdd2 = sc.parallelize(sample2, 1)
    
    calculate(rdd1, rdd2)
    //rdd1.join(rdd2).map(x => x._1 -> reduce(x._2._1, x._2._2)).foreach(println)
  }

  def save(sparkSession: SparkSession, day: String) {
    val previousDay = DateTimeUtil.create(day, "yyyy-MM-dd").minusDays(1).toString("yyyy-MM-dd")
    val sc = sparkSession.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    if (HdfsUtil.isExist(fs, s"/data/radius/stats/${previousDay}/download-upload")) {
      val rdd1 = sc.textFile(s"/data/radius/stats/${previousDay}/download-upload", 1).map(x => new DownUp(x.split("\t")))
      val rdd2 = sc.textFile(s"/data/radius/stats/${day}/download-upload", 1).map(x => new DownUp(x.split("\t")))
      val result = calculate(rdd1, rdd2)
      result.saveAsTextFile(s"/data/radius/stats/${day}/diff")
    }
  }
  
  /**
   * Tính download-usage và upload-usage của ngày này so với ngày trước đó.
   */
  private def calculate(rdd1: RDD[DownUp], rdd2: RDD[DownUp]): RDD[Difference] = {
    val pair1 = rdd1.map(x => x.toPair)
    val pair2 = rdd2.map(x => x.toPair)
    
    // Lấy tất cả những record trong rdd2 nhưng không trong rdd1, giá trị download/upload diff sẽ bằng với giá trị download/upload
    val subtract = pair2.subtractByKey(pair1).map(x => Difference(x._1, x._2._1, x._2._2, x._2._1, x._2._2))
    // Join rdd1 và rdd2 theo name sau đó lấy download/upload của rdd2 trừ rdd1
    val join = pair2.join(pair1).map(x => new Difference(x._1, x._2._1, reduce(x._2._2, x._2._1)))
    (subtract ++ join)
  }
  
  private def reduce(e1: (Long, Long), e2: (Long, Long)): (Long, Long) = {
    (e2._1 - e1._1, e2._2 - e1._2)
  }
}