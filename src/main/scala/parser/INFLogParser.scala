package parser

import java.io.{BufferedReader, FileInputStream, InputStreamReader, RandomAccessFile}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

/**
  * Created by hungdv on 19/06/2017.
  */
class INFLogParser extends AbtractLogParser{
  private val text =  "(.*)"

  val time = "(\\d{2}:\\d{2}:\\d{2})"
//  val date = "(\\w{3,}  \\d{1,})"
  val date = "(\\w{3,}\\s{1,}\\d{1,})"

  val timeWithDate = "(\\w{3,}\\s{1,}\\d{1,} \\d{2}:\\d{2}:\\d{2})"

  val hostName = "(\\w{4}\\d{5}\\w{2}\\d{2})"
  // search 0 1
  val module1 = "(0/\\d{1})"
  // search 3
  val module2 = "(\\d{1})"
  // search 5 6 7
  val module3 = "( \\d{1}/\\d{1}/\\d{1,3})"

/*  val searchObj0 = s"$text$timeWithDate$text$hostName$text %DEVICE-3-LINKUPDOWN: p$module1 LinkDown.".r
  val searchObj1 = s"$text$timeWithDate$text$hostName$text %DEVICE-3-LINKUPDOWN: e$module1 LinkDown.".r
  val searchObj2 = s"$text$timeWithDate$text$hostName$text %OAM-5-CPU_BUSY: cpu is busy.".r
  val searchObj3 = s"$text$timeWithDate$text$hostName$text %DEVICE-5-POWER-MANAGE: Power running no good detected, power NO : $module2.".r
  val searchObj4 = s"$text$timeWithDate$text$hostName$text %OAM-5-RELOAD_SUCCESSFULLY: reboot device successfully".r
  val searchObj5 = s"$text$timeWithDate$text$hostName$text$module3$text deregister reason sf".r
  val searchObj6 = s"$text$timeWithDate$text$hostName$text$module3$text deregister reason lofi".r
  val searchObj7 = s"$text$timeWithDate$text$hostName$text$module3$text power off".r*/
  val searchObj0 = s"$text$date $time$text$hostName$text %DEVICE-3-LINKUPDOWN: p$module1 LinkDown.".r
  val searchObj1 = s"$text$date $time$text$hostName$text %DEVICE-3-LINKUPDOWN: e$module1 LinkDown.".r
  val searchObj2 = s"$text$date $time$text$hostName$text %OAM-5-CPU_BUSY: cpu is busy.".r
  val searchObj3 = s"$text$date $time$text$hostName$text %DEVICE-5-POWER-MANAGE: Power running no good detected, power NO : $module2.".r
  val searchObj4 = s"$text$date $time$text$hostName$text %OAM-5-RELOAD_SUCCESSFULLY: reboot device successfully".r
  val searchObj5 = s"$text$date $time$text$hostName$text$module3$text deregister reason sf".r
  val searchObj6 = s"$text$date $time$text$hostName$text$module3$text deregister reason lofi".r
  val searchObj7 = s"$text$date $time$text$hostName$text$module3$text power off".r
  def extractValues(line: String): Option[AbtractLogLine] ={
    line match{
      case searchObj0(text,date,time,text2,hostName,text3,module) => Option (InfLogLineObject("user port down",hostName,stringToStandardDate(date),time,module))
      case searchObj1(text,date,time,text2,hostName,text3,module) => Option( InfLogLineObject("inf port down",hostName,stringToStandardDate(date),time,module))
      case searchObj2(text,date,time,text2,hostName,text3) => Option(InfLogLineObject("high cpu",hostName,stringToStandardDate(date),time,"-"))
      case searchObj3(text,date,time,text2,hostName,text3,module) => Option(InfLogLineObject("power error",hostName,stringToStandardDate(date),time,"0/"+module))
      case searchObj4(text,date,time,text2,hostName,text3) => Option(InfLogLineObject("reboot device",hostName,stringToStandardDate(date),time,"-"))
      case searchObj5(text,date,time,text2,hostName,text3,module,text4) => Option(InfLogLineObject("module/cpe error",hostName,stringToStandardDate(date),time,module))
      case searchObj6(text,date,time,text2,hostName,text3,module,text4) => Option(InfLogLineObject("disconnect/lost IP",hostName,stringToStandardDate(date),time,module))
      case searchObj7(text,date,time,text2,hostName,text3,module,text4) => Option(InfLogLineObject("power off",hostName,stringToStandardDate(date),time,module))
      case _ => None
    }
  }
  def stringToStandardDate(date: String):String ={
    val string = Calendar.getInstance().get(Calendar.YEAR) + " " + date
    val sdf  = new SimpleDateFormat("yyyy MMM dd")
    val sdf2 = new SimpleDateFormat("yyyy/MM/dd")
    val date1  = sdf.parse(string)
    val result  = sdf2.format(date1)
    result.toString
  }

}

case class  InfLogLineObject(
                           val logType: String,
                           val hostName: String,
                           val date: String,
                           val time: String,
                           val module: String
                           ) extends AbtractLogLine{

}
object DateAndTimeTest{
  def main(args: Array[String]): Unit = {

    val string = Calendar.getInstance().get(Calendar.YEAR) + " Jun  1"
    println(string)
    val sdf  = new SimpleDateFormat("yyyy MMM dd")
    val sdf2 = new SimpleDateFormat("yyyy/MM/dd")
    val date  = sdf.parse(string)
    println(date)
    val result  = sdf2.format(date)
    println(result)

    val strings = List("<131>0000061849: Jun  1 08:36:10: TNNP03202GC57: %DEVICE-3-LINKUPDOWN: e0/1 LinkDown.",
      "<131>0000084755: Jun  4 22:29:10: TNNP04202GC57: %DEVICE-3-LINKUPDOWN: p0/7 LinkDown.",
      "<134>0000190476: Jun  1 10:12:45: TNNP03301GC57: %ONTMNT-6-Informational: 3461158:75: 2017/06/01 10:12:45 ont 0/2/31 CIGGe4359131 deregister reason sf",
      "<134>0000019825: Jun  1 07:29:44: HPGP09002GC57: %ONTMNT-6-Informational: 696246:49: 2017/06/01 07:29:44 ont 0/2/49 CIGGf2127457 deregister reason lofi",
      "<134>0000152007: Jun  1 07:39:28: HNIP51601GC57: %ONTMNT-6-Informational: 11939729:83: 2017/06/01 07:39:28 ont 0/3/117 FPTT1690202f power off",
      "<133>0000142260: Jun  1 21:28:47: HNIP31102GC57: %DEVICE-5-POWER-MANAGE: Power running no good detected, power NO : 1.",
      "<133>0000160516: Jun  2 00:14:19: HNIP35201GC57: %OAM-5-CPU_BUSY: cpu is busy.",
      "<133>0000160516: Jun  2 00:14:19: HNIP35201GC57: %OAM-5-RELOAD_SUCCESSFULLY: reboot device successfully",
      "<190>0001064412: 0001304034: Jun 27 12:34:34: HCMP34204GC57: %ONTMNT-6-Informational: 49372214:56: 2017/06/27 12:34:34 ont 0/3/67 CIGGf4306498 deregister reason sf"
    )

    val parser = new INFLogParser
    strings.foreach { string =>
      val parsed = parser.extractValues(string)
      System.out.println(parsed)

    }


    var count : Int= 0
    var error : Int= 0

    //val rdacFile = new RandomAccessFile("/home/hungdv/infmb-2017-06-01_first100k.txt","rw")
/*
    val ponter = 0
    val total = rdacFile.length()
    for(a <- 0 until total.toInt){
      rdacFile.seek(a)
      val string = rdacFile.readLine()
      val parsed = parser.extractValues(string)
      if (parsed == None){count + 1}
      else println(parsed.get)
    }
*/

   /* val path: Path = java.nio.file.Paths.get("/home/hungdv/inf_message_debug.log")
    //val path: Path = java.nio.file.Paths.get("/home/hungdv/inf_deregister.log")
    //val path: Path = java.nio.file.Paths.get("/home/hungdv/infmb-2017-06-01_first100k.txt")
    val lines: util.List[String] = Files.readAllLines(path,StandardCharsets.UTF_8)
    val total = lines.size()
    for(a <- 0 until total){
      val string = lines.get(a)
      val parsed = parser.extractValues(string)
   /*   if (parsed != None){count + 1}
      else println(parsed.get)*/
      parsed match{
        case Some(x) =>{ count = count.+(1)}
        case _ => error = error.+(1)
      }
    }
    println("count/total/error : " + count + " / " + total + "/" + error)*/

  }
}