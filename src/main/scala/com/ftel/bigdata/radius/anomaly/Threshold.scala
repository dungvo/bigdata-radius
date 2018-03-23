package com.ftel.bigdata.radius.anomaly

import com.ftel.bigdata.conf.Configure
import org.apache.spark.sql.{SaveMode, SparkSession}

object Threshold {
  val ROOT_PATH = "/data/radius/stats-con"
  val OUTPUT_PATH = "/data/radius/threshold-week"
  val LOCAL_PATH = "/home/thanhtm/2018-03-??"

  def main(args: Array[String]): Unit = {
    val sc = Configure.getSparkContext()
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    sc.textFile(s"$ROOT_PATH/2018-03-??")
      .map(_.split("\t"))
      .filter(_.length == 9)
      .map(x => (x(6), x(8)))
      .toDF("signIn", "logOff")
      .select($"signIn"./($"logOff").alias("SL"), $"logOff"./($"signIn").alias("LS"))
      .na.drop()
      .createOrReplaceTempView("table")

    val threshold = spark.sql(s"SELECT PERCENTILE(SL, 0.95), PERCENTILE(LS, 0.95) FROM table").collect()

    sc.makeRDD(threshold, 1)
      .map(x => (x.getDouble(0).toInt, x.getDouble(1).toInt))
      .map(_.productIterator.mkString("\t"))
      .toDF()
      .write
      .mode(SaveMode.Overwrite)
      .csv(OUTPUT_PATH)
  }
}
