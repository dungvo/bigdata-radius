package com.ftel.bigdata.radius.classify

case class ErrLog(text: String) extends AbstractLog {
  override def get() = "ErrLog"
}