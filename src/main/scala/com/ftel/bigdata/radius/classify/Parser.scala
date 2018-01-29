package com.ftel.bigdata.radius.classify

object Parser {
  def apply(line: String): AbstractLog = {
    val loadLog = LoadLog(line)
    if (loadLog.isInstanceOf[ErrLog]) ConLog(line) else loadLog
  }
}