package parser

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
/**
  * Created by hungdv on 11/05/2017.
  */
object OptionParserOnFail {
  private val master = "local[2]"
  private val appName = "OptionParserOnFail"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.set("es.port","9200")
      .set("es.nodes","localhost")
      .set("es.http.timeout","5m")
      .set("es.scroll.size","50")
      .set("es.index.auto.create", "true")
      .setMaster("local[2]")
      .setAppName("EsTest")
    val sparkSession = SparkSession.builder().config(sparkConf).appName(appName).master(master).getOrCreate()
//    val df = sparkSession.createDataFrame(Seq((1,3,4),(1,2,3),(2,3,4),(2,3,5))).toDF("col1","col2","col3")
//    df.show()
//    val exprs = df.columns.map((_ -> "approx_count_distinct")).toMap
//
//    df.agg(exprs).show()
    val rdd = sparkSession.sparkContext.parallelize(Seq(
        "10:59:59 00001335 Auth-Local:Reject: Sgfdl-150825-040, Result 5, Out Of Office (70:d9:31:56:e0:be)",
        "10:59:59 00000423 Auth-Local:SignIn: Kgdsl-130727-700, KGG-MP01-1, 64AFA38A",
        "10:20:00 00000322 Auth-Local:LogOff: Hnfdl-150622-796, HN-MP02-5, 5C6678BE",
        "FailCase"))

    val connLogParser = new ConnLogParser
    val indexName     = "radius"
    val `typeName`    = "bras"

    /// OKEY
 /*   rdd.map{
      line => connLogParser.extractValues(line).getOrElse(None)
    }.filter(x => x!= None).collect().foreach(println(_))*/
    // OKEY
    val connRDD = rdd.map{
      line =>
        val parsedOBject = connLogParser.extractValues(line).getOrElse(None)
        parsedOBject match{
          case Some(x) => x.asInstanceOf[ConnLogLineObject]
          case _  => None
        }
        parsedOBject

    }.filter(x => x!= None)
   //.map(ob => ob.asInstanceOf[ConnLogLineObject])
   //.map(ob => converObjectToMap(ob))
    // NOT Okey
/*    rdd.map{
      line => connLogParser.extractValues(line).getOrElse(null)
    }.collect().foreach(println(_))*/
    val numbers = Map("time" -> "10:59:59", "session_id" -> "00001335", "connect_type" -> "Reject","name" -> "Sgfdl-150825-040","content1" -> "Result 5","content2" -> "Out Of Office (70:d9:31:56:e0:be)")
    connRDD.foreach(println(_))
    //connRDD.saveToEs("es_radius_3/case_class")
    sparkSession.sparkContext.makeRDD(Seq(numbers)).saveToEs("radius/conn")
  }

}
