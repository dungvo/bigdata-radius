package com.ftel.bigdata.radius.streaming

import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.utils.JsonObject
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import org.joda.time.DateTimeZone

case class RadiusMessage(timeStamp: Long, message: String, topic: String, partition: Int, offset: Long) {
  def this(json: JsonObject, topic: String, partition: Int, offset: Long) = this(
      
//      DateTime.parse(
//          json.getValueAsString("@timestamp"), 
//          DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
//          .withZone(DateTimeZone.forID(DateTimeUtil.TIMEZONE_HCM)).plusHours(7).getMillis,
      // Format timestamp in kafka is UTC. So +07:00 hour
      DateTimeUtil.create(json.getValueAsString("@timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").plusHours(7).getMillis,
      json.getValueAsString("message"),
      topic,
      partition,
      offset)
}

object RadiusMessage {
  def main(args: Array[String]) {
    
  }
  
}