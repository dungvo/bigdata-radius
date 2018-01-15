package util

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Duration}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by hungdv on 16/04/2017.
  */
object DatetimeController extends Serializable{
  /**
    * Return list of date from date1 to date2.
    * @param startDate
    * @param endDate
    * @return ArrayBuffer[Datetime] List of date
    */
  def getDateList(startDate: DateTime,endDate: DateTime): ArrayBuffer[DateTime]={
    var result =  new ArrayBuffer[DateTime]()
    val duration = new Duration(startDate,endDate).getStandardDays.toInt
    var i = 0
    for(i <- 0 until  duration){
        val currentDate = startDate.plusDays(i)
        result.append(currentDate)
    }
    result
  }
  def sqlTimeStampToNumberFormat(time: java.sql.Timestamp): Float = {
    val cal: Calendar = Calendar.getInstance()
    cal.setTimeInMillis(time.getTime)
    //val year =  cal.get(Calendar.YEAR)
    //val month = cal.get(Calendar.MONTH)
    //val date = cal.get(Calendar.DATE)
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    val minute = cal.get(Calendar.MINUTE)
    val result = (minute.toFloat/100) + hour
    result
  }
  def stringToTime(string: String,pattern: String): Timestamp = {
    val date: DateTime = DateTime.parse(string,DateTimeFormat.forPattern(pattern) )
    val timstamp = new Timestamp(date.getMillis)
    timstamp
  }
  def stringToLong(s: String, timePattern: String) :Long = {
    val sdf = new SimpleDateFormat(timePattern)
    val ms = sdf.parse(s).getTime()
    ms
  }

  def stringWithTimeZoneToSqlTimestamp(string: String): Timestamp ={
    val date: DateTime = DateTime.parse(string,DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ") )
    val timstamp = new Timestamp(date.getMillis)
    timstamp
  }

 def stringToSqlTimestamp(string: String): Timestamp ={
    val date: DateTime = DateTime.parse(string,DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS") )
    val timstamp = new Timestamp(date.getMillis)
    timstamp
  }

 def stringToLong(s: String): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val ms = sdf.parse(s).getTime()
    ms
  }

 def  stringWithTimeZoneToSqlTimestamp(string: String,pattern: String): String ={
   val parsed = DatetimeController.stringWithTimeZoneToSqlTimestamp(string)
   val result = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(parsed)
   result
 }

  def  stringWithTimeZoneToSqlTimestamp(string: String,formater: SimpleDateFormat): String ={
    val parsed = DatetimeController.stringWithTimeZoneToSqlTimestamp(string)
    val result = formater.format(parsed)
    result
  }

  def stringToDate(string: String, pattern: String) : Date = {
    val  formatter = new SimpleDateFormat(pattern)
    val date = formatter.parse(string)
    date
  }
  def stringToDatime(string: String,pattern: String): DateTime ={
    val date: DateTime = DateTime.parse(string,DateTimeFormat.forPattern(pattern) )
    date
  }
  def getNow() = {
    val now = new org.joda.time.DateTime();
    val pattern = "yyyy-MM-dd_hh.mm.ss";
    val formatter = DateTimeFormat.forPattern(pattern);
    val formatted = formatter.print(now);
    formatted
  }


}
object DateTimeTest{
  def main(args: Array[String]): Unit = {
    val string = "2017-08-28T14:56:59.000+07:00"


    val result = string.substring(0,16).replace("T"," ")
    println(result)
    val parsed = DatetimeController.stringWithTimeZoneToSqlTimestamp(string)

    val date: DateTime = DateTime.parse(string,DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ") )
    println(date)

    val array = new ArrayBuffer[String]()
    for(i <- 1 to  60){
      array.append("'download-" + i + "'")
    }
    val seq :Seq[String]= array.toList
    println(seq)



    //println (new SimpleDateFormat("yyyy-MM-dd HH:mm").format(parsed))
  }
}
