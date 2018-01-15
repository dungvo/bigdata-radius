package batch_jobs.netflow

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.joda.time.DateTime
import org.apache.spark.sql.functions._

/**
  * Created by hungdv on 05/01/2018.
  */
object ParseNetflow {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("akka").setLevel(Level.OFF)
    val logger = Logger.getLogger(getClass)
    val sparkConf = new SparkConf()
    val sparkSession = SparkSession.builder()
      .appName("batch_jobs.ParserNetflow")
      .master("yarn")
      .getOrCreate()
    import sparkSession.implicits._
    val sparkContext = sparkSession.sparkContext
    val dateTime  =  DateTime.now().minusHours(10).toString("yyyyMMdd")
    val path = s"/data/netflow/netflow_data.json-${dateTime}"
    val output = s"/data/netflow/netflow_data-${dateTime}.csv"


    val json: DataFrame = sparkSession.read.json(path)

    val netFlow = new StructType()
      .add("netflow",new StructType()
        .add("dst_as", LongType )
        .add("dst_mask", LongType  )
        .add("engine_id", LongType  )
        .add("engine_type", LongType  )
        .add("first_switched", StringType  )
        .add("flow_records", LongType   )
        .add("flow_seq_num", LongType  )
        .add("in_bytes", LongType  )
        .add("in_pkts", LongType  )
        .add("input_snmp", LongType  )
        .add("ipv4_dst_addr", StringType  )
        .add("ipv4_next_hop", StringType  )
        .add("ipv4_src_addr", StringType  )
        .add("l4_dst_port", LongType  )
        .add("l4_src_port", LongType  )
        .add("last_switched", StringType  )
        .add("output_snmp", LongType )
        .add("protocol", LongType )
        .add("sampling_algorithm", LongType )
        .add("sampling_interval", LongType )
        .add("src_as", LongType )
        .add("src_mask", LongType )
        .add("src_tos", LongType )
        .add("tcp_flags", LongType))
      .add("@version", StringType )
      .add("@timestamp", StringType )
      .add("host",StringType)


    val df = json.select(from_json($"message",netFlow) as "msg")
    val csv = df.select("msg.netFlow.*","msg.host","msg.@version","msg.@timestamp")
    csv.repartition(1).write
      .option("header","true")
        .mode("overwrite")
      .csv(output)
    deleteJson(path,sparkContext)

  }
  def deleteJson(path: String,sc: SparkContext) = {
    import org.apache.hadoop.fs.FileSystem
    import org.apache.hadoop.fs.Path
    val fs=FileSystem.get(sc.hadoopConfiguration)

    if(fs.exists(new Path(path)))
      fs.delete(new Path(path),true)
  }

}

object ParseNetflowLocalTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("akka").setLevel(Level.OFF)
    val logger = Logger.getLogger(getClass)
    val sparkConf = new SparkConf()
    val sparkSession = SparkSession.builder()
      .appName("batch_jobs.ParserNetflow")
      .master("local[2]")
      //.master("yarn")
      .getOrCreate()
    import sparkSession.implicits._
    val sparkContext = sparkSession.sparkContext
    val dateTime  =  DateTime.now().minusHours(300).toString("yyyyMMdd")

    val path = "/home/hungdv/Desktop/json.sample"


    val netFlow = new StructType()
      .add("netflow",new StructType()
        .add("dst_as", LongType )
        .add("dst_mask", LongType  )
        .add("engine_id", LongType  )
        .add("engine_type", LongType  )
        .add("first_switched", StringType  )
        .add("flow_records", LongType   )
        .add("flow_seq_num", LongType  )
        .add("in_bytes", LongType  )
        .add("in_pkts", LongType  )
        .add("input_snmp", LongType  )
        .add("ipv4_dst_addr", StringType  )
        .add("ipv4_next_hop", StringType  )
        .add("ipv4_src_addr", StringType  )
        .add("l4_dst_port", LongType  )
        .add("l4_src_port", LongType  )
        .add("last_switched", StringType  )
        .add("output_snmp", LongType )
        .add("protocol", LongType )
        .add("sampling_algorithm", LongType )
        .add("sampling_interval", LongType )
        .add("src_as", LongType )
        .add("src_mask", LongType )
        .add("src_tos", LongType )
        .add("tcp_flags", LongType))
      .add("@version", StringType )
      .add("@timestamp", StringType )
      .add("host",StringType)

    val json: DataFrame = sparkSession.read.schema(netFlow).json(path)
    json.printSchema()
    json.select("host","@timestamp","netflow.*").show()

    /*val netFlow = new StructType()
      .add("netflow",new StructType()
        .add("dst_as", LongType )
        .add("dst_mask", LongType  )
        .add("engine_id", LongType  )
        .add("engine_type", LongType  )
        .add("first_switched", StringType  )
        .add("flow_records", LongType   )
        .add("flow_seq_num", LongType  )
        .add("in_bytes", LongType  )
        .add("in_pkts", LongType  )
        .add("input_snmp", LongType  )
        .add("ipv4_dst_addr", StringType  )
        .add("ipv4_next_hop", StringType  )
        .add("ipv4_src_addr", StringType  )
        .add("l4_dst_port", LongType  )
        .add("l4_src_port", LongType  )
        .add("last_switched", StringType  )
        .add("output_snmp", LongType )
        .add("protocol", LongType )
        .add("sampling_algorithm", LongType )
        .add("sampling_interval", LongType )
        .add("src_as", LongType )
        .add("src_mask", LongType )
        .add("src_tos", LongType )
        .add("tcp_flags", LongType)
        .add("@version", StringType )
        .add("host",StringType))

    val df = json.select(from_json($"message",netFlow) as "msg")
    val csv = df.select($"msg.netFlow.*")
    csv.repartition(1).write
      .option("header","true")
      .mode("overwrite")
      .csv(output)
    deleteJson(path,sparkContext)*/

  }
  def deleteJson(path: String,sc: SparkContext) = {
    import org.apache.hadoop.fs.FileSystem
    import org.apache.hadoop.fs.Path
    val fs=FileSystem.get(sc.hadoopConfiguration)

    if(fs.exists(new Path(path)))
      fs.delete(new Path(path),true)
  }

}


