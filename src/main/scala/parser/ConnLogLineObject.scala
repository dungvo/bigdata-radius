package parser

import java.sql.Date
import java.text.SimpleDateFormat

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by hungdv on 08/03/2017.
  */
/**
  *
  * @param time
  * @param session_id
  * @param connect_type
  * @param name
  * @param content1 [SignIn-LogOff - Content1 ~ NASName ] [Reject- Content1 ~ rejectCause]
  * @param content2 [unidentified or rejrectResult]
  */
case class ConnLogLineObject(
                              time:      String,
                              session_id:    String,
                              connect_type:   String,
                              name:      String,
                              content1:  String,
                              content2:  String
                      ) extends AbtractLogLine{

}

object ConnLogLineObject
{
  def create( time:      String,
             session_id:    String,
             connect_type:   String,
             name:      String,
             content1:  String,
             content2:  String): ConnLogLineObject = {
    val datetime: String = DateTime.parse(extractKafkaTimeStampFromContent2(content2) + " " +  time  , DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")).toString("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
    //val datetime: String = DateTime.parse(DateTime.now().toString("yyyy-MM-dd") + " " +  time  , DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")).toString("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
      ConnLogLineObject(datetime, session_id, connect_type, name, content1, content2)
  }

  /**
    * Extract timestamp (kafkam message's timestamp - long) from content2.
    * It was appended to content2.
    * @param content2
    * @return
    */
  def extractKafkaTimeStampFromContent2(content2: String) : String ={
    val timestamp = content2.substring(content2.lastIndexOf("-")+1).toLong
    convertTime(timestamp)
  }
  def convertTime(time :Long ): String={
    val date = new Date(time)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val string = format.format(date)
    string
  }

  def main(args: Array[String]): Unit = {
    val content2  = "06:59:59 00000108 Auth-Local:Reject: hndsl-141106-438, Result 6, Account Has Been Closed (4c:f2:bf:44:27:b2)-1297380023295"
    val time      = "06:59:59"
    val datetime: String = DateTime.parse(extractKafkaTimeStampFromContent2(content2) + " " +  time  , DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")).toString("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
    println(datetime)
  }
}
