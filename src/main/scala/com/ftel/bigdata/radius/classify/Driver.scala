package com.ftel.bigdata.radius.classify

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.joda.time.DateTime
import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.conf.Configure
import scala.util.matching.Regex

object Driver {

  def main(args: Array[String]) {
    run(args)
    //Parser.runLocal()
  }
  
  def run(args: Array[String]) {
    val flag = args(0)
    val day = args(1)

    val sc = Configure.getSparkContext()

    flag match {
      case "month" => {
        val dateTime = DateTimeUtil.create(day, DateTimeUtil.YMD).dayOfMonth().withMinimumValue()
        val number = dateTime.dayOfMonth().getMaximumValue()
        (0 until number).map(x => {
          Parser.run(sc, dateTime.plusDays(x).toString(DateTimeUtil.YMD))
          //Merge.run(sc, dateTime.plusDays(x).toString(DateTimeUtil.YMD))
        })
      }
      case "month-classify-merge" => {
        val dateTime = DateTimeUtil.create(day, DateTimeUtil.YMD).dayOfMonth().withMinimumValue()
        val number = dateTime.dayOfMonth().getMaximumValue()
        (0 until number).map(x => {
          Parser.run(sc, dateTime.plusDays(x).toString(DateTimeUtil.YMD))
          Merge.run(sc, dateTime.plusDays(x).toString(DateTimeUtil.YMD))
        })
      }
      case "day" => {
        Parser.run(sc, day)
        //Merge.run(sc, day)
      }
      case "day-classify-merge" => {
        Parser.run(sc, day)
        Merge.run(sc, day)
      }
      case "merge-month" => {
        val dateTime = DateTimeUtil.create(day, DateTimeUtil.YMD).dayOfMonth().withMinimumValue()
        val number = dateTime.dayOfMonth().getMaximumValue()
        (0 until number).map(x => {
//          Parser.run(sc, dateTime.plusDays(x).toString(DateTimeUtil.YMD))
          Merge.run(sc, dateTime.plusDays(x).toString(DateTimeUtil.YMD))
        })
      }
      case "merge-day" => {
//        Parser.run(sc, day)
        Merge.run(sc, day)
      }
      case _ => {
        println("Wrong parameters.")
      }
    }
  }

//  def runLocal() {
//    val path = "radius-log-sample.csv"
//    val sc = Configure.getSparkContextLocal()
//    val lines = sc.textFile(path, 1)
//    
//    val filter = lines
//      .filter(x => !matchRegex(x, ConLog.REGEX01))
//      .filter(x => !matchRegex(x, ConLog.REGEX02))
//    
//    filter.foreach(println)
//    println("LINE: " + lines.count())
//    println("COUNT: " + filter.count())
//  }
  
  private def matchRegex(s: String, regex: Regex): Boolean = {
    s match {
      case regex(_*) => true
      case _ => false
    }
  }
}