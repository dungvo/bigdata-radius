package com.ftel.bigdata.radius.classify

import org.joda.time.DateTime
import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.utils.Parameters

case class RawLog(timestamp: Long, text: String) extends AbstractLog {
  
  def this(text: String) = this(0, text)
  //def this(day: String, line: String) = this(DateTimeUtil.create(day + " " + DateTimeUtil.now.toString("HH:mm:ss"), Parser.DATE_TIME_PATTERN_NOMALIZE), line)
  
    
  override def toString = timestamp + "\t" + text
  
  override def get() = "err"
  override def getKey() = {
    val date = DateTimeUtil.create(timestamp / 1000L)
    "day=" + date.toString("yyyy-MM-dd") +
    "/type=" + get() +
    "/hour=" + date.toString("HH") +
    "/" + date.toString("HH-mm") +
    "-" + (date.toString("ss").toInt / 4).toString()
  }
  override def getTimestamp(): Long = timestamp
  override def toES = Map(
      "type" -> "raw",
      "timestamp" -> DateTimeUtil.create(timestamp / 1000L).toString(Parameters.ES_5_DATETIME_FORMAT),
      "text" -> text)
}

object RawLog {
  def apply(line: String): RawLog = {
    val arr = line.split("\t")
    if (arr.length >= 2) {
      RawLog(arr(0).toLong, arr.slice(1, arr.length).mkString("\t"))
    } else {
      new RawLog(line)
    }
  }
  
  def main(args: Array[String]) {
    val line = "1520272740254	07:59:00 0000E604 Auth-Local:Reject: Ppdsl-150804-659, Result 5, Out Of Office (60:e3:27:2f:d7:4b)"
    val raw = RawLog(line)
    println()
    
    val date = DateTimeUtil.create(raw.timestamp / 1000L).plusHours(7)
    
    println(date)
    
    
    
    
  }
}