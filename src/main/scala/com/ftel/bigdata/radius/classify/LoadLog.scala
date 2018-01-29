package com.ftel.bigdata.radius.classify

import com.ftel.bigdata.utils.StringUtil
import scala.util.matching.Regex
import com.ftel.bigdata.radius.utils.BrasUtil

case class LoadLog(
  statusType: String,
  date: String,
  nasName: String,           // bras Name
  nasPort: String,
  name: String,              // customer Name
  sessionID: String,
  input: String,             // (Upload)
  output: String,            // (Download)
  termCode: String,
  sessTime: String,
  ipAddress: String,
  callerID: String,          // MacAdddress
  ipv6Address: String,
  inputG: String,            // So Vong Upload
  outputG: String,           // So Vong Download
  inputIPv6: String,
  inputIPv6G: String,
  outputIPv6: String,
  outputIPv6G: String) extends AbstractLog {
  override def get() = "LoadLog"
  override def toString = Array(
      statusType,
      date,
      nasName,
      nasPort,
      name,
      sessionID,
      input,
      output,
      termCode,
      sessTime,
      ipAddress,
      callerID,
      ipv6Address,
      inputG,
      outputG,
      inputIPv6,
      inputIPv6G,
      outputIPv6,
      outputIPv6G).mkString("\t")
}


object LoadLog {
//  private val time1         = "(\\w{3,} \\d{2} \\d{4} \\d{2}:\\d{2}:\\d{2})"
  
  //val PATTERN: Regex = s"(\\w{3,} \\d{2} \\d{4} \\d{2}:\\d{2}:\\d{2})".r

  private val SIZE_LENGTH = 19
  def apply(line: String): AbstractLog = {
    val arr = line.split(",")//.map(x => BrasUtil.trim(x, "\""))
    //println(arr.mkString("\t"))
    if (arr.length == SIZE_LENGTH) {
      //try {
      val nomalize = arr.map(x => BrasUtil.trim(x, "\""))
      val i = new AutoIncrease(-1)
      LoadLog(
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get),
          nomalize(i.get))
      //} catch {
      //  case e: Exception => println(line); null
      //}
    } else ErrLog(line)
  }
  
//  private def trim(origin: String, omitted: String): String = {
//    if (StringUtil.isNullOrEmpty(origin)) origin
//    else {
//      if (origin.startsWith(omitted) && origin.endsWith(omitted)) {
//        origin.substring(omitted.length(), origin.length() - omitted.length())
//      } else if (origin.startsWith(omitted)) {
//        origin.substring(omitted.length())
//      } else if (origin.endsWith(omitted)) {
//        origin.substring(0, origin.length() - omitted.length())
//      } else {
//        origin
//      }
//    }
//  }
}

