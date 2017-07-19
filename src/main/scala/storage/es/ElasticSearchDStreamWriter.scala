package storage.es

import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.spark._
import storage.StorageWriter
import org.apache.spark.sql.DataFrame
import scala.reflect.ClassTag
/**
  * Created by hungdv on 17/03/2017.
  */
/**
  * Persist DStream to Elastic Search.
  * Use supported driver by ES.
  * Don't need to rewrite any things.
  *
  * @param dstream
  * @tparam T RDD type needs to be a Map, JavaBean,or Scala case class ( content can be translated into documents)
  */
class ElasticSearchDStreamWriter[T: ClassTag](@transient private val dstream: DStream[T] )extends StorageWriter{
  private val defaultIndexName: String  = "spark_streaming_default_index"
  private val defaultTypeName: String   = "spark_streaming_default_type"

  override def persistToStorage(storageConfig: Map[String, String]): Unit = {
    val indexName     = storageConfig.getOrElse("index", defaultIndexName)
    val `typeName`      = storageConfig.getOrElse("type",defaultTypeName)
    dstream.foreachRDD{
      rdd =>
        //
        // rdd must be not null, or else -> org.elasticsearch.hadoop.EsHadoopIllegalArgumentException:
        // Cannot determine write shards for [rdd_string_withouTimeStamp/test];
        // likely its format is incorrect (maybe it contains illegal characters
        if(rdd.count() > 0) {
          rdd.saveToEs(indexName + "/" + `typeName`)
        }
    }
  }
  override def persistToStorageDaily(storageConfig: Map[String, String]): Unit = {
    val indexPrefixName  = storageConfig.getOrElse("indexPrefix", defaultIndexName)

    val `typeName`       = storageConfig.getOrElse("type",defaultTypeName)
    dstream.foreachRDD{
      rdd =>
        if(rdd.count() > 0) {
          //val indexName  = (indexPrefixName + "-" + org.joda.time.DateTime.now().toString("yyyy-MM-dd"))
          //rdd.saveToEs("without_time_parsed2" + "/" + `typeName`)
          rdd.saveToEs((indexPrefixName + "-" + org.joda.time.DateTime.now().toString("yyyy-MM-dd")) + "/" + `typeName`)

        }
    }
  }
}

object ElasticSearchDStreamWriter{
  import scala.language.implicitConversions
  implicit def createESWriter[T: ClassTag](dstream: DStream[T]): ElasticSearchDStreamWriter[T] = {
    new ElasticSearchDStreamWriter[T](dstream)
  }
}

