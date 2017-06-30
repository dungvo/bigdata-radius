package util

import java.sql.Timestamp
import java.util.Calendar

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
    val year =  cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH)
    val date = cal.get(Calendar.DATE)
    val hour = cal.get(Calendar.HOUR)
    val minute = cal.get(Calendar.MINUTE)
    val result = (minute.toFloat/100) + hour
    result
  }
}
object DateTimeTest{
  def main(args: Array[String]): Unit = {
    println(" : " + DatetimeController.sqlTimeStampToNumberFormat(new Timestamp(2017,6,30,8,22,32,11)))
  }
}
