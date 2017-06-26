package util

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
}
