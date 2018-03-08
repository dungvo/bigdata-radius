package com.ftel.bigdata.radius.stats

import com.ftel.bigdata.radius.classify.LoadLog
import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.utils.Parameters

case class LoadStats(timestamp: Long, name: String, sessionId: String, sessionTime: Long, download: Long, upload: Long) {
  private def this(arr: Array[String]) = this(arr(0).toLong, arr(1), arr(2), arr(3).toLong, arr(4).toLong, arr(5).toLong)
  def this(line: String) = this(line.split("\t"))
  override def toString() = Array(timestamp, name, sessionId, sessionTime, download, upload).mkString("\t")
  def toES() = {
    Map(
      "timestamp" -> DateTimeUtil.create(timestamp / 1000L).toString(Parameters.ES_5_DATETIME_FORMAT),
      "name" -> name, 
      "sessionId" -> sessionId,
      "sessionTime" -> sessionTime, 
      "download" -> download, 
      "upload" -> upload)
  }
}

object LoadStats {
  val MAX_INT = Integer.MAX_VALUE * 2L + 1
  val THRESHOLD = 102400000L//threshold
  def apply(log: LoadLog): LoadStats = {
    val download = log.output + log.outputG * MAX_INT
    val upload = log.input + log.inputG * MAX_INT
    LoadStats(log.timestamp, log.name, log.sessionID, log.sessionTime, download, upload)
  }
  
  def main(args: Array[String]) {
//    println(Integer.MAX_VALUE * 2L)
    val line = """ "ACTALIVE","Dec 01 2017 06:59:59","LDG-MP01-2","796176075","Lddsl-161001-360","1905765","477268962","3712614232","0","1011598","100.91.231.187","64:d9:54:82:37:e4","","1","35","0","0","0","0" """
    val log = LoadLog(line, 0L).asInstanceOf[LoadLog]
    val stats = LoadStats(log)
    println(log)
    println(stats)
  }
}