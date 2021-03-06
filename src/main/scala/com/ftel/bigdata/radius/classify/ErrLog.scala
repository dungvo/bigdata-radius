package com.ftel.bigdata.radius.classify

import org.joda.time.DateTime
import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.utils.Parameters

case class ErrLog(timestamp: Long, text: String) extends AbstractLog {
  
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
      "type" -> "err",
      "timestamp" -> DateTimeUtil.create(timestamp / 1000L).toString(Parameters.ES_5_DATETIME_FORMAT),
      "text" -> text)
}

object ErrLog {
  def apply(line: String): ErrLog = {
    val arr = line.split("\t")
    if (arr.length >= 2) {
      ErrLog(arr(0).toLong, arr.slice(1, arr.length).mkString("\t"))
    } else {
      new ErrLog(line)
    }
  }
}