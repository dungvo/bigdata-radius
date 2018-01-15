package test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by hungdv on 27/07/2017.
  */
object DataFrameTest {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
    val sc = sparkSession.sparkContext
    import sparkSession.implicits._
    // TEST split strubg.
    val df = sparkSession.sparkContext.parallelize(Seq(("MX480/0/12/4","foo","bar"))).toDF("port","foo","bar")
    df.createOrReplaceTempView("count")
    val resutl = sparkSession.sql("SELECT *,split(port, '/')[0] as part1, split(port, '/')[1] as part2 FROM count")
    resutl.show()

    val bras_result3_ids: Array[Any] = resutl.select("port").rdd.map(r => r(0)).collect()
    bras_result3_ids.toList.foreach(println(_))

    import org.apache.spark.sql.functions._
    val timeTest = resutl.withColumn("time",lit(current_timestamp()))
    timeTest.show
  }
}
object CompareOperationTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
    val sc = sparkSession.sparkContext
    import sparkSession.implicits._

    val df = sparkSession.sparkContext.parallelize(Seq(("A",8,"1"),("A",9,null),("B",9,"1"))).toDF("id","num","count")
    df.show()
    val notNull = df.where($"count".isNotNull)
    notNull.show()
    val null_ = df.where($"count".isNull)
    null_.show()
    val filter_true = df.filter(x => x.getAs[String]("count") != "1")
    filter_true.show()

  }
}