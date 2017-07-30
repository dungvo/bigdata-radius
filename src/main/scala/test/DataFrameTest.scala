package test

import org.apache.spark.sql.SparkSession

/**
  * Created by hungdv on 27/07/2017.
  */
object DataFrameTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
    val sc = sparkSession.sparkContext
    import sparkSession.implicits._
    // TEST split strubg.
    val df = sparkSession.sparkContext.parallelize(Seq(("MX480/0/12/4","foo","bar"))).toDF("port","foo","bar")
    df.createOrReplaceTempView("count")
    val resutl = sparkSession.sql("SELECT *,split(port, '/')[0] as part1, split(port, '/')[1] as part2 FROM count")
    resutl.show()
  }
}
