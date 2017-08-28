package test

import core.udafs.GroupByStringAgg
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{KeyValueGroupedDataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window

/**
  * Created by hungdv on 28/08/2017.
  */
object String_AGG_Test {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().appName("Test agg string").master("local").getOrCreate()
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
    val result  = df.withColumn("agg",stringAGG(col("string")).over(window2))
    sparkSession.sqlContext.udf.register("gm", new GroupByStringAgg)
    val gm = new GroupByStringAgg
    //wont work
    //val result2 = df.groupBy("id").agg(gm(col("string")).as("mkdir"))

    result.show()
    //result2.show()


    val logOff = rdd.groupByKey().map(x => (x._1, x._2.toSet.mkString(",")))
    logOff.foreach(println(_))

  }

}
