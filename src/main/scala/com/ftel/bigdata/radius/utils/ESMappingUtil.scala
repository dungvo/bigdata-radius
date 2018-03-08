package com.ftel.bigdata.radius.utils

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.ElasticDsl.CreateIndexExecutable
import com.sksamuel.elastic4s.http.ElasticDsl.CreateIndexTemplateHttpExecutable
import com.sksamuel.elastic4s.http.ElasticDsl.RichFuture
import com.sksamuel.elastic4s.http.ElasticDsl.createIndex
import com.sksamuel.elastic4s.http.ElasticDsl.createTemplate
import com.sksamuel.elastic4s.http.ElasticDsl.intField
import com.sksamuel.elastic4s.http.ElasticDsl.keywordField
import com.sksamuel.elastic4s.http.ElasticDsl.mapping

import com.ftel.bigdata.utils.ESUtil
import org.elasticsearch.common.settings.Settings

object ESMappingUtil {
  
  private val ES_HOST = "172.27.11.156"
  private val ES_PORT = 9200
  
  /**
   * 
PUT _template/template_radius-streaming
{
  "template": "radius-streaming-*",
  "order": 0,
  "settings": {
    "refresh_interval": "60s",
    "number_of_shards": 3,
    "number_of_replicas": 0
  },
  "mappings": {
      "con": {
        "properties": {
          "day": {
            "type": "keyword"
          },
          "domain": {
            "type": "keyword"
          },
          "label": {
            "type": "keyword"
          },
          "graphModel": {
            "type": "keyword"
          },
          "featureModel": {
            "type": "keyword"
          },
          "nameModel": {
            "type": "keyword"
          }
        }
      }
    }
}
   */
  @deprecated("Code hien tai chay ko duoc, loi parse json mapping, su dung PUT")
  def createMappingRadius() {
    val client = ESUtil.getClient(ES_HOST, ES_PORT)
    //client.execute {
    val req = 
      createTemplate("radius-streaming", "radius-streaming-*")
        .mappings(
          mapping("docs") as {keywordField("aaaaa")}
        )
        //.settings(new Settings())
      
          //.shards(3).replicas(0).refreshInterval("10s")
    //}.await
            
    client.execute(req).await
        //println(req.mappings)
    //println(client.show(req))
    client.close()
    
  }
  
  def main(args: Array[String]) {
    println("=============")
    createMappingRadius()
  }
}