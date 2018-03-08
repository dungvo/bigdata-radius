package com.ftel.bigdata.radius.classify

import org.joda.time.DateTime

trait AbstractLog {
  def get(): String
  def getKey(): String
  def getTimestamp(): Long
  def toES(): Map[String, Any]
}

//object AbstractLog {
//  def apply(line: String): AbstractLog = {
//    val size = line.split(" ")
//    if (size == 1) {
//      
//    } else {
//      
//    }
//  }
//}