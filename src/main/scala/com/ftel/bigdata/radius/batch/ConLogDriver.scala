package com.ftel.bigdata.radius.batch

import org.apache.spark.sql.SparkSession
import com.ftel.bigdata.utils.Parameters
import org.apache.spark.SparkContext
import com.ftel.bigdata.utils.StringUtil
import org.apache.spark.storage.StorageLevel
import com.ftel.bigdata.radius.classify.ConLog
import java.util.Formatter.DateTime
import com.ftel.bigdata.utils.DateTimeUtil
import org.apache.spark.rdd.RDD


case class Bras(
    date: String,
    bras: String,
    linecard: Int,
    card: Int,
    port: Int,
    signInUnique: Int,
    signIn: Int,
    logoffUnique: Int,
    logoff: Int) {
  override def toString = Array(
      date,
      bras,
      linecard,
      card,
      port,
      signInUnique,
      signIn,
      logoffUnique,
      logoff).mkString("\t")
}
    
object ConLogDriver {
  def main(args: Array[String]) {
//    runLocal()
    run(args)
  }
  
  private def runLocal() {
    val sparkSession = SparkSession.builder().appName("LOCAL").master("local[2]").getOrCreate()
    val sc = sparkSession.sparkContext
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    sc.hadoopConfiguration.set(Parameters.SPARK_READ_DIR_RECURSIVE, "true")
    
    groupByBras(sparkSession, sc, "")
  }
  
  private def run(args: Array[String]) {
    
    val flag = args(0)
    val day = args(1)
    
    
    val sparkSession = SparkSession.builder().getOrCreate()
    val sc = sparkSession.sparkContext
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    sc.hadoopConfiguration.set(Parameters.SPARK_READ_DIR_RECURSIVE, "true")
    
    flag match {
      case "type" => groupByType(sc, day)
      case "bras" => groupByBras(sparkSession, sc, day)
    }
//    
    
  }
  
  private def groupByType(sc: SparkContext, day: String) {

    val path = s"/data/radius/partition/con/${day}"
    val output = s"/data/radius/stats-con/${day}/type"

    val conLogs = sc.textFile(path, 1)
      .filter(x => StringUtil.isNotNullAndEmpty(x))
      .map(x => ConLog(x))
      .map(x => (DateTimeUtil.create(x.timestamp / 1000), x.name.toUpperCase(), x.nasName.toUpperCase(), x.typeLog))
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    conLogs.map(x => (x._1.toString(DateTimeUtil.YMD), x._1.toString("HH"), x._2, x._3 ) -> toTriple(x._4) )
           .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
           .map(x => Array(x._1._1, x._1._2, x._1._3, x._1._4, x._2._1, x._2._2, x._2._3).mkString("\t"))
           .coalesce(32, false, None)
           .saveAsTextFile(output)
  }
  
  def toTriple(_type: String): (Int, Int, Int) = {
      _type match {
        case ConLog.REJECT._2 => (1, 0, 0)
        case ConLog.SIGNIN._2 => (0, 1, 0)
        case ConLog.LOGOFF._2 => (0, 0, 1)
      }
    }
  
  private def groupByBras(sparkSession: SparkSession, sc: SparkContext, day: String) {
    val path = s"/data/radius/partition/con/${day}"
    //val check = "bnh-mp-01-02"
    //val path = "/home/dungvc/workspace/bigdata-radius/conlog-parse.csv"
    val output = s"/data/radius/stats-con/${day}/bras"

    val rdd = groupBrasByMinutes(
        sparkSession,
        sc.textFile(path, 1)
          .filter(x => StringUtil.isNotNullAndEmpty(x))
          .map(x => ConLog(x)))
    rdd.coalesce(32, false, None).saveAsTextFile(output)
  }

  def groupBrasByMinutes(sparkSession: SparkSession, rdd: RDD[ConLog]) = {
    import sparkSession.implicits._
    val df = rdd
      .filter(x => x.typeLog != ConLog.REJECT._2)
      .map(x => (
          DateTimeUtil.create(x.timestamp / 1000).toString(DateTimeUtil.YMD + " HH:mm:00"),
          x.nasName.toUpperCase(),
          x.card.lineId,
          x.card.id,
          x.card.port,
          x.name,
          x.typeLog))
      .toDF("date", "bras", "linecard", "card", "port", "name", "type")
    df.createOrReplaceTempView("Con")
    val group = s"SELECT date, bras, linecard, card, port, type, COUNT(DISTINCT name) as unique, COUNT(name) as count " +
                "FROM Con " +
                "GROUP BY date,bras,linecard,card,port,type"
    val rs = sparkSession.sql(group)
    def row2col(x: (String, Long, Long) ): (Long, Long, Long, Long) = {
      x._1 match {
        case ConLog.SIGNIN._2 => (x._2, x._3, 0, 0)
        case ConLog.LOGOFF._2 => (0, 0, x._2, x._3)
      }
    }
    rs.rdd.map(x => {
        (x.getAs[String]("date"), x.getAs[String]("bras"), x.getAs[Int]("linecard"), x.getAs[Int]("card"), x.getAs[Int]("port")) ->
        row2col(x.getAs("type"), x.getAs("unique"), x.getAs("count"))
      })
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4) )
      
      .map(x => Bras(
          x._1._1.trim(),
          x._1._2.trim(),
          x._1._3,
          x._1._4,
          x._1._5,
          x._2._1.toInt,
          x._2._2.toInt,
          x._2._3.toInt,
          x._2._4.toInt))
  }
}