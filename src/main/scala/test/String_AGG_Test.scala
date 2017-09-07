package test

import java.util.Properties

import batch_jobs.anomaly_detection.ThresholdCalculator.getClass
import core.udafs.GroupByStringAgg
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{KeyValueGroupedDataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.IntegerType
import storage.postgres.PostgresIO

/**
  * Created by hungdv on 28/08/2017.
  */
object String_AGG_Test {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val logger = Logger.getLogger(getClass)

    val sparkConf = new SparkConf().set("spark.cassandra.connection.host","172.27.11.156")
      .set("spark.cassandra.output.batch.size.rows","auto")

    val sparkSession = SparkSession.builder().appName("batch_jobs.CalculateBrasThreshold")
      //.master("yarn")
      .master("local[3]")
      //.config(sparkConf)
      .getOrCreate()
    val postgresConfig = Predef.Map("jdbcUsername" -> "dwh_noc",
      "jdbcPassword" -> "bigdata",
      "jdbcHostname" -> "172.27.11.153",
      "jdbcPort"     -> "5432",
      "jdbcDatabase" -> "dwh_noc")
    val jdbcURL = PostgresIO.getJDBCUrl(postgresConfig)
    val bJdbcURL = sparkSession.sparkContext.broadcast(jdbcURL)
    val pgProperties = new Properties()
    pgProperties.setProperty("driver", "org.postgresql.Driver")
    val bPgProperties = sparkSession.sparkContext.broadcast(pgProperties)

    val seq = Seq((1,"foo"),(1,"bar"),(1,"far"),(1,"far"),(2,"boo"),(2,"foa"))
    val rdd = sparkSession.sparkContext.parallelize(seq)
    import sparkSession.implicits._

    val df = rdd.toDF("id","string")
    df.show()
    import org.apache.spark.sql.functions._
    import core.udafs.String_AGG
    val stringAGG = new String_AGG
    val window2 = Window.partitionBy("id")
    //val result = df.groupBy("id").agg(stringAGG(col("string")))
/*    val result  = df.withColumn("agg",stringAGG(col("string")).over(window2))
    sparkSession.sqlContext.udf.register("gm", new GroupByStringAgg)
    val gm = new GroupByStringAgg
    //wont work
    //val result2 = df.groupBy("id").agg(gm(col("string")).as("mkdir"))

    result.select("id","agg").dropDuplicates().show()
    //result2.show()
    val logOff = rdd.groupByKey().map(x => (x._1, x._2.toSet.mkString(",")))
    logOff.foreach(println(_))*/

    val  cardQuery = " (select distinct(card_ol),bras_id from bras_count_by_card)  as tm "
    val  lineQuery = " (select distinct(line_ol),bras_id from bras_count_by_card) as tm "
    val windowByBras = Window.partitionBy("bras_id")
    val distinctLine = PostgresIO.pushDownQuery(sparkSession = sparkSession,bJdbcURL.value,lineQuery,bPgProperties.value)
    val distinctCard = PostgresIO.pushDownQuery(sparkSession = sparkSession,bJdbcURL.value,cardQuery,bPgProperties.value)
    /////////////////////// OLD method -> list
    //val distinctLineAdded = distinctLine.select(concat(lit("'Line "),col("line_ol"),lit("'")).as("line_ol"),col("bras_id"))
    //val distinctCardAdded = distinctCard.select(concat(lit("'Card "),col("card_ol"),lit("'")).as("card_ol"),col("bras_id"))


    //val lineResult = distinctLineAdded.withColumn("agg",stringAGG(col("line_ol")).over(windowByBras))
    //  .select("bras_id","agg").dropDuplicates().withColumnRenamed("agg","line_ol_list")
    // val lineResult = distinctLine.groupBy("bras_id").agg(stringAGG(col("card_ol")))
    //val cardResult = distinctCardAdded.withColumn("agg",stringAGG(col("card_ol")).over(windowByBras))
    //  .select("bras_id","agg").dropDuplicates().withColumnRenamed("agg","card_ol_list")

    ///////////////////////// NEW method -> list max.
    val lineResult = distinctLine.withColumn("line_ol",distinctLine("line_ol").cast(IntegerType))
                                  .withColumn("line_ol_list",max("line_ol").over(windowByBras))
                                  .drop("line_ol").dropDuplicates()

    val cardResult = distinctCard.withColumn("card_ol",distinctCard("card_ol").cast(IntegerType))
      .withColumn("card_ol_list",max("card_ol").over(windowByBras))
      .drop("card_ol").dropDuplicates()


    val result = lineResult.join(cardResult,"bras_id")

    try{
      PostgresIO.writeToPostgres(sparkSession,result,bJdbcURL.value,"linecard_ol",SaveMode.Overwrite,bPgProperties.value)
    }catch{
      case e: Exception => println("Error mother fucker !")
      case _ => println("Dont give a shit.")
    }
  }

}
