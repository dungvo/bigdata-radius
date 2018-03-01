package com.ftel.bigdata.radius.utils

import com.ftel.bigdata.utils.FileUtil
import com.ftel.bigdata.utils.StringUtil

case class BrasMapping(
    region: String,
    ip: String,
    hostnameNoc: String,
    hostnameRadius: String,
    hostnameOpsview: String,
    hostnameKibana: String)

object BrasUtil {
  private val BRAS_MAPPING = getBrasMapping()
  private val BRAS_RADIUS_MAPPING = BRAS_MAPPING.map(x => x.hostnameRadius -> x.hostnameNoc).toMap
  
  def getHostnameNoc(hostnameRadius: String): String = {
    BRAS_RADIUS_MAPPING.getOrElse(trim(hostnameRadius.trim().toLowerCase(), ","), "N/A")
  }
  
  private def getBrasMapping(): Array[BrasMapping] = {
    val lines = FileUtil.readResource("/com/ftel/bigdata/radius/utils/bras_mapping.csv")
    lines.filter(x => !x.startsWith("#"))
         .map(x => x.toLowerCase().split(","))
         .filter(x => x.length == 6)
         .map(x => BrasMapping(x(0), x(1), x(2), x(3), x(4), x(5)))
         .toArray
  }
  
  def trim(origin: String, omitted: String): String = {
    if (StringUtil.isNullOrEmpty(origin)) origin
    else {
      if (origin.startsWith(omitted) && origin.endsWith(omitted)) {
        origin.substring(omitted.length(), origin.length() - omitted.length())
      } else if (origin.startsWith(omitted)) {
        origin.substring(omitted.length())
      } else if (origin.endsWith(omitted)) {
        origin.substring(0, origin.length() - omitted.length())
      } else {
        origin
      }
    }
  }
  
  def main(args: Array[String]) {
    BRAS_MAPPING.foreach(println)
    //val path = getBrasMapping()
  }
}