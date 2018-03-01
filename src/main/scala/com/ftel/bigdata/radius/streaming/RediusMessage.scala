package com.ftel.bigdata.radius.streaming

import com.ftel.bigdata.utils.DateTimeUtil

case class RadiusMessage(timeStamp: Long, message: String, topic: String, partition: Int, offset: Long) {
  def this(json: JsonObject, topic: String, partition: Int, offset: Long) = this(
      
      DateTimeUtil.create(json.getValueAsString("@timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").getMillis,
      json.getValueAsString("message"),
      topic,
      partition,
      offset)
}