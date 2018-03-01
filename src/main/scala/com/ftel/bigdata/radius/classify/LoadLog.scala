package com.ftel.bigdata.radius.classify

import com.ftel.bigdata.utils.StringUtil
import scala.util.matching.Regex
import com.ftel.bigdata.radius.utils.BrasUtil
import org.joda.time.DateTime
import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.utils.Parameters.TAB

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
  override def get() = "load"
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
  override def getKey() = {
    val date = DateTimeUtil.create(timestamp / 1000L)
    "day=" + date.toString("yyyy-MM-dd") +
    "/type=" + get() +
    "/hour=" + date.toString("HH") +
    "/" + date.toString("mm")
  }
}


object LoadLog {

  val DATE_TIME_PATTERN = "MMM dd yyyy HH:mm:ss" // "Dec 01 2017 06:59:59"
  
  private val SIZE_LENGTH = 19
  def apply(line: String, day: String): AbstractLog = {
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
      // "Dec 01 2017 19:57:54","MX480-03","-2065400882","Ppdsl-130725-138","44820762","504227037","-509709527","10","259360","100.66.140.102","18:a6:f7:ec:94:87"
      val nomalize = arr.map(x => BrasUtil.trim(x, "\""))
      //val i = new AutoIncrease(-1)
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
          -1,
          -1,
          -1,
          -1,
          -1,
          -1)
    }
    else new ErrLog(day, line)
  }

  def apply(line: String): LoadLog = {
    val arr = line.split(TAB)
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
      arr(i.get).toInt,
      arr(i.get).toInt,
      arr(i.get).toLong,
      arr(i.get).toInt,
      arr(i.get).toLong,
      arr(i.get).toInt)
  }
  
  def main(args: Array[String]) {
    val line = """ "ACTALIVE","Dec 01 2017 06:59:59","HCM-MP06-1","1244642428","Sgdsl-120214-182","89884947","1897988750","3264444589","0","237589","100.105.70.164","4c:f2:bf:b7:8e:e6","2405:4800:5a97:1a1b:0000:0000:0000:0000/64","0","7","0","0","0","0" """
    //val date = DateTimeUtil.create("Dec 01 2017 06:59:59", "MMM dd yyyy HH:mm:SS")
    //println(date.toString("yyyy-MM-dd HH:mm:SS"))
    val log = LoadLog(line, "2017-12-01")
    println(DateTimeUtil.create(log.asInstanceOf[LoadLog].timestamp).toString("MMM dd yyyy HH:mm:SS"))
    val line1 = log.toString()
    println(line1)
    val log2 = LoadLog(line1)
    val line2 = log2.toString()
    println(line2)
  }
}

