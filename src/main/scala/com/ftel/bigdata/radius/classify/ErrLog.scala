package com.ftel.bigdata.radius.classify

import org.joda.time.DateTime
import com.ftel.bigdata.utils.DateTimeUtil

case class ErrLog(date: DateTime, text: String) extends AbstractLog {
  override def get() = "err"
  def this(day: String, line: String) = this(DateTimeUtil.create(day + " " + DateTimeUtil.now.toString("HH:mm:ss"), Parser.DATE_TIME_PATTERN_NOMALIZE), line)
  override def getKey() = 
    "day=" + date.toString("yyyy-MM-dd") +
    "/type=" + get() +
    "/hour=" + date.toString("HH") +
    "/" + date.toString("HH-mm") +
    "-" + (date.toString("ss").toInt / 4).toString()
    
  override def toString = text
}