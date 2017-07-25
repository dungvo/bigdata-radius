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

  private val interface = "(\\d{1,2}/\\d{1,2}/\\d{1,3}).(\\d{1,4}):(\\d{1,4})"
  private val olt = ("[A-Z]{4}[a-zA-Z0-9]{9}")

  private val portPON = "(\\d{1}/\\d{1}/\\d{1,3})"
  private val text = "(.*)"
  private val vlan = "(\\d{1,4})"
  private val serialONU = ("[A-Z]{4}[a-z0-9]{8}")
  private val macAddStandard = "(\\w{2}:\\w{2}:\\w{2}:\\w{2}:\\w{2}:\\w{2})"
  private val macAdd = "(\\w{12})"
  private val extension = s"xe-$interface#$olt PON $portPON $macAdd $vlan $serialONU$text".r

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
  //TODO : add time stamp - [date + time] at the end of msg then you subtract it again ?! you are so funny.
  def convertTime(time :Long ): String={
    val date = new Date(time)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val string = format.format(date)
    string
  }
  def parserExtensionLog(content2: String): (String,String,String,String,String,String) ={
    content2 match{
      case extension(interface,olt,portPON,macAdd,vlan,serialONU,text) => (interface,olt,portPON,macAdd,vlan,serialONU)
      case _ => (null,null,null,null,null,null)
    }
  }

  def main(args: Array[String]): Unit = {
    val content2  = "06:59:59 00000108 Auth-Local:Reject: hndsl-141106-438, Result 6, Account Has Been Closed (4c:f2:bf:44:27:b2)-1297380023295"
    val time      = "06:59:59"
    val datetime: String = DateTime.parse(extractKafkaTimeStampFromContent2(content2) + " " +  time  , DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")).toString("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
    println(datetime)

    val ex1 = "xe-0/1/0.3173:3173#HNIP20201GC57 PON 0/2/20 9c50ee294836 3173 FPTT16b06773, B4C4A2D4"
    val ex2 = "xe-5/3/0.3116:3116#HNIP08001GC57 PON 0/4/96 a858403b2d2e 3116 FPTT15c0fe1c, 3ED5A7F9"
    val ex3 = "xe-1/2/0.713:713#TNNP04701GC57 PON 0/1/5 70d931655686 713 CIGGf3157865, 74B5D444"
    val ex4 = "xe-8/0/1.3418:3418#HNIP51602GC57 PON 0/5/104 a8584001b86e 3418 CIGGf4601075, 7FC3E9E5"
    println(parserExtensionLog(ex1))
    println(parserExtensionLog(ex2))
    println(parserExtensionLog(ex3))
    println(parserExtensionLog(ex4))


  }
}
