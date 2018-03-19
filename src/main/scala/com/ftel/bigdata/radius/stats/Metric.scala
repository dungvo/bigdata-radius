package com.ftel.bigdata.radius.stats

import org.apache.spark.sql.SparkSession
import com.ftel.bigdata.utils.DateTimeUtil
import org.apache.hadoop.fs.FileSystem
import com.ftel.bigdata.utils.HdfsUtil
import org.apache.spark.rdd.RDD
import com.ftel.bigdata.conf.Configure
import org.apache.spark.SparkContext

//case class Metric(day: String, name: String, session: String, download: Long, upload: Long, duration: String)

object Metric {
  
  def save(sparkSession: SparkSession, day: String) {
    val previousDay = DateTimeUtil.create(day, "yyyy-MM-dd").minusDays(1).toString("yyyy-MM-dd")
    val sc = sparkSession.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val prevPath = s"/data/radius/stats/${previousDay}/load-usage"
    val currPath = s"/data/radius/stats/${day}/load-usage"

    if (HdfsUtil.isExist(fs, prevPath)) {
      val prev = sc.textFile(prevPath, 1).map(x => new LoadStats(x))
      val curr = sc.textFile(currPath, 1).map(x => new LoadStats(x))
      val result = calculate(prev, curr)
      result.saveAsTextFile(s"/data/radius/stats/${day}/metric")
    } else {
      sc.textFile(currPath, 1)
        .map(x => new LoadStats(x))
        .coalesce(16, false, None)
        .saveAsTextFile(s"/data/radius/stats/${day}/metric")
    }
  }
  
  def cal(sparkSession: SparkSession, sc: SparkContext, day: String): RDD[LoadStats] =  {
    val previousDay = DateTimeUtil.create(day, DateTimeUtil.YMD).minusDays(1).toString(DateTimeUtil.YMD)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val prevPath = s"/data/radius/stats/${previousDay}/load-usage"
    val currPath = s"/data/radius/stats/${day}/load-usage"
    val output = s"/data/radius/stats/${day}/metric"
    if (!HdfsUtil.isExist(fs, output)) {
      val result = if (HdfsUtil.isExist(fs, prevPath)) {
        val prev = sc.textFile(prevPath, 1).map(x => new LoadStats(x))
        val curr = sc.textFile(currPath, 1).map(x => new LoadStats(x))
        calculate(prev, curr)
      } else {
        sc.textFile(currPath, 1)
          .map(x => new LoadStats(x))
          .coalesce(16, false, None)
      }
      result.saveAsTextFile(output)
      result
    } else {
      sc.textFile(output, 1).map(x => new LoadStats(x))
    }
  }
  
  /**
   * Tính download-usage, upload-usage, and duration của ngày này so với ngày trước đó.
   */
  private def calculate(prev: RDD[LoadStats], curr: RDD[LoadStats]): RDD[LoadStats] = {
    val pair1 = prev.map(x => (x.name + x.sessionId) -> x)
    val pair2 = curr.map(x => (x.name + x.sessionId) -> x)
    
    // Lấy tất cả những record trong curr nhưng không trong prev, giá trị download/upload diff sẽ bằng với giá trị download/upload
    val subtract = pair2.subtractByKey(pair1).map(x => x._2)
    // Join rdd1 và rdd2 theo name sau đó lấy download/upload của rdd2 trừ rdd1
    val join = pair2.join(pair1).map(x => reduce(x._2._2, x._2._1))
    (subtract ++ join)
  }
  
  private def reduce(e1: LoadStats, e2: LoadStats): LoadStats = {
    //(e2._1 - e1._1, e2._2 - e1._2)
    if (e1.timestamp > e2.timestamp) {
      LoadStats(e1.timestamp, e1.name, e1.sessionId, e1.sessionTime - e2.sessionTime, e1.download - e2.download, e1.upload - e2.upload)
    } else {
      LoadStats(e2.timestamp, e2.name, e2.sessionId, e2.sessionTime - e1.sessionTime, e2.download - e1.download, e2.upload - e1.upload)
    }
  }
  
  def main(args: Array[String]) {
    val prev = Array(
          LoadStats(1, "C1", "A", 10, 10, 20),
          LoadStats(2, "C2", "B", 20, 10, 30),
          LoadStats(3, "C2", "C", 30, 20, 30)
        )
        
    val curr = Array(
          LoadStats(4, "C1", "A", 20, 60, 90),
          LoadStats(5, "C1", "D", 10, 10, 30),
          LoadStats(6, "C2", "C", 40, 50, 60),
          LoadStats(6, "C3", "F", 40, 50, 60)
        )
        
    val sc = Configure.getSparkContextLocal()
    
    val rdd1 = sc.parallelize(prev, 1)
    val rdd2 = sc.parallelize(curr, 1)
    
    val rs = calculate(rdd1, rdd2).collect()
    
    val c1a = rs.find(x => x.name == "C1" && x.sessionId == "A").get
    val c2c = rs.find(x => x.name == "C2" && x.sessionId == "C").get
    
    
    assert(c1a.sessionTime == 10)
    assert(c1a.download == 50)
    assert(c1a.upload == 70)
    
    assert(c2c.sessionTime == 10)
    assert(c2c.download == 30)
    assert(c2c.upload == 30)
    //rdd1.join(rdd2).map(x => x._1 -> reduce(x._2._1, x._2._2)).foreach(println)
  }
}