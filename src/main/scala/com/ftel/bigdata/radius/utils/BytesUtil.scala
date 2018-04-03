package com.ftel.bigdata.radius.utils

object BytesUtil {
  def bytesToGigabytes(bytes: Long): Double = {
    bytesTo(bytes, true, 3)
  }
  
  def bytesToTerabytes(bytes: Long): Double = {
    bytesTo(bytes, true, 4)
  }
  
  def bytesToPetabytes(bytes: Long): Double = {
    bytesTo(bytes, true, 5)
  }
  
  def bytesTo(bytes: Long, si: Boolean, exp: Int): Double = {
    val unit = if (si) 1000 else 1024
    val number = bytes / java.lang.Math.pow(unit, exp)
    BigDecimal(number).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}