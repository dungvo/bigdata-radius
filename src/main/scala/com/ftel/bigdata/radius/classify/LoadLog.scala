package com.ftel.bigdata.radius.classify

import com.ftel.bigdata.utils.StringUtil
import scala.util.matching.Regex
import com.ftel.bigdata.radius.utils.BrasUtil
import org.joda.time.DateTime
import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.utils.Parameters.TAB
import com.ftel.bigdata.utils.Parameters
import com.ftel.bigdata.radius.stats.LoadStats

case class LoadLog(
  statusType: String,
  timestamp: Long,           // unit is millis
  nasName: String,           // bras Name
  nasPort: Int,
  name: String,              // customer Name
  sessionID: String,
  input: Long,             // (Upload)
  output: Long,            // (Download)
  termCode: Int,
  sessionTime: Long,
  ipAddress: String,
  callerID: String,          // MacAdddress
  ipv6Address: String,
  inputG: Int,            // So Vong Upload
  outputG: Int,           // So Vong Download
  inputIPv6: Long,
  inputIPv6G: Int,
  outputIPv6: Long,
  outputIPv6G: Int) extends AbstractLog {
  
  override def toString = Array(
      statusType,
      timestamp,
      nasName,
      nasPort,
      name,
      sessionID,
      input,
      output,
      termCode,
      sessionTime,
      ipAddress,
      callerID,
      ipv6Address,
      inputG,
      outputG,
      inputIPv6,
      inputIPv6G,
      outputIPv6,
      outputIPv6G).mkString("\t")
  override def get() = "load"
  override def getKey() = {
    val date = DateTimeUtil.create(timestamp / 1000L)
    "day=" + date.toString("yyyy-MM-dd") +
    "/type=" + get() +
    "/hour=" + date.toString("HH") +
    "/" + date.toString("mm")
  }
  override def getTimestamp(): Long = timestamp
  override def toES = {
    val loadStats = LoadStats(this)
    Map(
      "type" -> "load",
      "timestamp" -> DateTimeUtil.create(timestamp / 1000L).toString(Parameters.ES_5_DATETIME_FORMAT),
      "statusType" -> statusType,
      "nasName" -> nasName.toLowerCase(),
      "nasPort" -> nasPort,
      "name" -> name.toLowerCase(),
      "sessionID" -> sessionID,
      "input" -> input,
      "output" -> output,
      "termCode" -> termCode,
      "sessionTime" -> sessionTime,
      "ipAddress" -> ipAddress,
      "callerID" -> callerID,
      "ipv6Address" -> ipv6Address,
      "inputG" -> inputG,
      "outputG" -> outputG,
      "inputIPv6" -> inputIPv6,
      "inputIPv6G" -> inputIPv6G,
      "outputIPv6" -> outputIPv6,
      "outputIPv6G" -> outputIPv6G,
      "download" -> loadStats.download,
      "upload" -> loadStats.upload)
      }
}


object LoadLog {

  val DATE_TIME_PATTERN = "MMM dd yyyy HH:mm:ss" // "Dec 01 2017 06:59:59"
  
  private val SIZE_LENGTH = 19
  def apply(line: String, timestamp: Long): AbstractLog = {
    val arr = line.trim().split(",")
    if (arr.length == SIZE_LENGTH) {
      val nomalize = arr.map(x => BrasUtil.trim(x, "\""))
      val i = new AutoIncrease(-1)
      LoadLog(
          nomalize(i.get),
          DateTimeUtil.create(nomalize(i.get), DATE_TIME_PATTERN).getMillis,
          nomalize(i.get),
          nomalize(i.get).toInt,
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get).toLong,
          nomalize(i.get).toLong,
          nomalize(i.get).toInt,
          nomalize(i.get).toLong,
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get).toInt,
          nomalize(i.get).toInt,
          nomalize(i.get).toLong,
          nomalize(i.get).toInt,
          nomalize(i.get).toLong,
          nomalize(i.get).toInt)
    } else if (arr.length == 11 || arr.length == 12) {
      //            "Dec 01 2017 19:57:54","MX480-03","-2065400882","Ppdsl-130725-138","44820762","504227037","-509709527","10","259360","100.66.140.102","18:a6:f7:ec:94:87"
      val nomalize = arr.map(x => BrasUtil.trim(x, "\""))
      //val i = new AutoIncrease(-1)
      //println(nomalize.mkString("\t"))
      LoadLog(
          "UNKNOWN",
          DateTimeUtil.create(nomalize(0), DATE_TIME_PATTERN).getMillis,
          nomalize(1),
          nomalize(2).toInt,
          nomalize(3),
          nomalize(4),
          nomalize(5).toLong,
          nomalize(6).toLong,
          nomalize(7).toInt,
          nomalize(8).toLong,
          nomalize(9),
          nomalize(10),
          null,
          0,
          0,
          0,
          0,
          0,
          0)
    } else if (arr.length == 13) {
      // "ACTALIVE","Oct 01 2017 06:59:59","KTM-MP01-2","-1639172390","ktdsl-140905-396","3116602","528510406","-904169345","0","739775","100.94.65.35","bc:96:80:36:1a:24","" 
      
      val nomalize = arr.map(x => BrasUtil.trim(x, "\""))
      val i = new AutoIncrease(-1)
      //println(nomalize.mkString("\t"))
      LoadLog(
          nomalize(i.get),
          DateTimeUtil.create(nomalize(i.get), DATE_TIME_PATTERN).getMillis,
          nomalize(i.get),
          nomalize(i.get).toInt,
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get).toLong,
          nomalize(i.get).toLong,
          nomalize(i.get).toInt,
          nomalize(i.get).toLong,
          nomalize(i.get),
          nomalize(i.get),
          null,
          0,
          0,
          0,
          0,
          0,
          0)
    } else new ErrLog(timestamp, line)
  }

  def apply(line: String): LoadLog = {
    val arr = line.split(TAB)
    def f(value: Int): Int = if (value > 0) value else 0
    def fL(value: Long): Long = if (value > 0) value else 0
    
    val i = new AutoIncrease(-1)
    LoadLog(
      arr(i.get),
      arr(i.get).toLong,
      arr(i.get),
      arr(i.get).toInt,
      arr(i.get),
      arr(i.get),
      arr(i.get).toLong,
      arr(i.get).toLong,
      arr(i.get).toInt,
      arr(i.get).toLong,
      arr(i.get),
      arr(i.get),
      arr(i.get),
      f(arr(i.get).toInt),
      f(arr(i.get).toInt),
      fL(arr(i.get).toLong),
      f(arr(i.get).toInt),
      fL(arr(i.get).toLong),
      f(arr(i.get).toInt))
  }
  
  def main(args: Array[String]) {
    //val line = """ "ACTALIVE","Dec 01 2017 06:59:59","HCM-MP06-1","1244642428","Sgdsl-120214-182","89884947","1897988750","3264444589","0","237589","100.105.70.164","4c:f2:bf:b7:8e:e6","2405:4800:5a97:1a1b:0000:0000:0000:0000/64","0","7","0","0","0","0" """
    val line = """ "ACTALIVE","Oct 01 2017 06:59:59","KTM-MP01-2","-1639172390","ktdsl-140905-396","3116602","528510406","-904169345","0","739775","100.94.65.35","bc:96:80:36:1a:24","" """
    //val date = DateTimeUtil.create("Dec 01 2017 06:59:59", "MMM dd yyyy HH:mm:SS")
    //println(date.toString("yyyy-MM-dd HH:mm:SS"))
    println(line)
    val log = LoadLog(line, 1512086400L)
    println(DateTimeUtil.create(log.asInstanceOf[LoadLog].timestamp).toString("MMM dd yyyy HH:mm:SS"))
    val line1 = log.toString()
    println(line1)
    val log2 = LoadLog(line1)
    val line2 = log2.toString()
    println(line2)
  }
}

