package batch_jobs.anomaly_detection

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.datastax.spark.connector.SomeColumns
import org.apache.spark.sql.cassandra._
/**
  * Created by hungdv on 30/06/2017.
  */
/**
  * Calculate threshold - 75Th of earch bras over lastweek
  * Save result to Cassandra
  * Somany hard code here
  */
object ThresholdCalculator {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val logger = Logger.getLogger(getClass)

    val sparkConf = new SparkConf().set("spark.cassandra.connection.host","172.27.11.156")
                                   .set("spark.cassandra.output.batch.size.rows","auto")

    val sparkSession = SparkSession.builder().appName("batch_jobs.CalculateBrasThreshold")
                                             .master("yarn")
                                            //.master("local[2]")
                                             .config(sparkConf).getOrCreate()
    val createDDL: String =
      """CREATE TEMPORARY VIEW brasscount
             USING org.apache.spark.sql.cassandra
             OPTIONS (
             table "brasscount",
             keyspace "radius",
             pushdown "true")"""
    val spark = sparkSession.sqlContext
    spark.sql(createDDL)

    val window2 = Window.partitionBy("bras_id")
    val now = System.currentTimeMillis()
    val timestamp = new org.joda.time.DateTime(now).minusDays(7).toString("yyyy-MM-dd HH:mm:ss.SSS")
    val q75TH = new core.udafs.Q75TH

    val brasCountLastWeek = spark.sql(s"SELECT * FROM brasscount WHERE time > '$timestamp'")
    logger.warn(s"Read brascount from $timestamp to $now")

    val brasThresHold = brasCountLastWeek.withColumn("threshold_signin",q75TH(col("signin_total_count")).over(window2))
                                         .withColumn("threshold_logoff",q75TH(col("logoff_total_count")).over(window2))
                                         .select("bras_id","threshold_signin","threshold_logoff")
    logger.warn(s"Calculate brascount threshold")

    brasThresHold.write.mode("append").cassandraFormat("bras_theshold","radius","test").save()
    logger.warn(s"Save bras threshold to Cassandra table : bras_theshold.")


  }
}


object ReadTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val logger = Logger.getLogger(getClass)

    val sparkConf = new SparkConf().set("spark.cassandra.connection.host","localhost")
      .set("spark.cassandra.output.batch.size.rows","auto")

    val sparkSession = SparkSession.builder().appName("batch_jobs.CalculateBrasThreshold")
      .master("local[2]").config(sparkConf).getOrCreate()
    val createDDL: String =
      """CREATE TEMPORARY VIEW brasscount
             USING org.apache.spark.sql.cassandra
             OPTIONS (
             table "brasscount",
             keyspace "radius",
             pushdown "true")"""

    val createDLLBrasThreshold  =
      """CREATE TEMPORARY VIEW brasscountthreshold
             USING org.apache.spark.sql.cassandra
             OPTIONS (
             table "bras_theshold",
             keyspace "radius",
             pushdown "true")"""

    val spark = sparkSession.sqlContext
    spark.sql(createDDL)
    spark.sql(createDLLBrasThreshold)
    import sparkSession.implicits._
    val df = sparkSession.sparkContext.parallelize(Seq(("bar",2),("bar2",1))).toDF("bras_id","count")
    df.createOrReplaceTempView("bras")
    val brasId = df.select("bras_id").rdd.map(r => r(0)).collect()
    var brasIdsString = "("
    brasId.foreach{x =>
      val y = "'" + x + "',"
      brasIdsString = brasIdsString + y
    }
    brasIdsString = brasIdsString.dropRight(1) + ")"

    val brasCountLastWeek = spark.sql(s"SELECT * FROM brasscount WHERE bras_id IN ${brasIdsString}")
    brasCountLastWeek.show()


  }
}
