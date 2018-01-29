package com.ftel.bigdata.radius.classify

trait AbstractLog {
  def get(): String
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