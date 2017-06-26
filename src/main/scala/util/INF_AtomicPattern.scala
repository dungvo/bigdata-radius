package util
import scala.util.matching.Regex
/**
  * Created by hungdv on 19/06/2017.
  */
object INF_AtomicPattern {
  private val text =  "(.*)"
  private val text2 =  "(.*)"
  private val text3 =  "(.*)"
  val time = "(\\d{2}:\\d{2}:\\d{2})"
  val timeWithDate = "(\\w{3,}  \\d{1,} \\d{2}:\\d{2}:\\d{2})"
  val hostName = "(\\w{4}\\d{5}\\w{2}\\d{2})"
  // search 0 1
  val module1 = "(0/\\d{1})"
  // search 3
  val module2 = "(\\d{1})"
  // search 5 6 7
  val module3 = "( \\d{1}/\\d{1}/\\d{1,3})"

  val searchObj0Content1  = "(: %DEVICE-3-LINKUPDOWN: e)"
  val searchObj0Conten2  = "( LinkDown.)"



  val searchObj0 = s"$text$timeWithDate$text$hostName$text %DEVICE-3-LINKUPDOWN: p$module1 LinkDown.".r
  val searchObj1 = s"$text$timeWithDate$text$hostName$text %DEVICE-3-LINKUPDOWN: e$module1 LinkDown.".r
  val searchObj2 = s"$text$timeWithDate$text$hostName$text %OAM-5-CPU_BUSY: cpu is busy.".r
  val searchObj3 = s"$text$timeWithDate$text$hostName$text %DEVICE-5-POWER-MANAGE: Power running no good detected, power NO : $module2.".r
  val searchObj4 = s"$text$timeWithDate$text$hostName$text %OAM-5-RELOAD_SUCCESSFULLY: reboot device successfully".r
  val searchObj5 = s"$text$timeWithDate$text$hostName$text$module3$text deregister reason sf".r
  val searchObj6 = s"$text$timeWithDate$text$hostName$text$module3$text deregister reason lofi".r
  val searchObj7 = s"$text$timeWithDate$text$hostName$text$module3$text power off".r

  def main(args: Array[String]): Unit = {
    val string1 = "<131>0000061849: Jun  1 08:36:10: TNNP03202GC57: %DEVICE-3-LINKUPDOWN: e0/1 LinkDown."
    val string2 = "<131>0000084755: Jun  4 22:29:10: TNNP04202GC57: %DEVICE-3-LINKUPDOWN: p0/7 LinkDown."

    val string3 = "<131>0000061849: Jun  1 08:36:10: TNNP03202GC57: %DEVICE-3-LINKUPDOWN: e0/1 LinkDown."

    // val string3Regext = s"$text$timeWithDate$text2$hostName$text3 e$module1 LinkDown.".r
    //
    val string3Regext = s"$text$timeWithDate$text$hostName$text %DEVICE-3-LINKUPDOWN: e$module1 LinkDown.".r
    val textPattern   =  "(.*)"
    val text2Pattern =  "(.*)"
    val text3Pattern   =  "(.*)"
    val timePattern = "(\\d{2}:\\d{2}:\\d{2})"
    val hostNamePattern = "(\\w{4}\\d{5}\\w{2}\\d{2})"
    val timeWithDatePattern = "(\\w{3,}  \\d{1,} \\d{2}:\\d{2}:\\d{2})"
    val module1sPattern = "(0/\\d{1})"
    val searchObjectContetn1Pattern = "(: %DEVICE-3-LINKUPDOWN: e)"
    val searchObjectContetn2Pattern = "( LinkDown.)"

    string3 match{
      case string3Regext(textPattern,timeWithDatePattern,text2Pattern,hostNamePattern2,text3Pattern,module1sPattern) => println("true text3 " + timeWithDatePattern + hostNamePattern2 + module1sPattern)
      case _ => println("None")
    }
    string1 match{
      case searchObj1(textPattern,timePattern,text2Pattern,hostNamePattern,searchObjectContetn1Pattern,modulePattern,searchObjectContetn2Pattern) => System.out.println("Found : ")
      case _ => println("None")
    }

    val stringob5 = "<134>0000190476: Jun  1 10:12:45: TNNP03301GC57: %ONTMNT-6-Informational: 3461158:75: 2017/06/01 10:12:45 ont 0/2/31 CIGGe4359131 deregister reason sf"
    stringob5 match{
      case searchObj5(text,time,text2,hostName,text3,module,text4) => println("match ob5")
      case _ => (println("None"))
    }
    val stringOb6 = "<134>0000019825: Jun  1 07:29:44: HPGP09002GC57: %ONTMNT-6-Informational: 696246:49: 2017/06/01 07:29:44 ont 0/2/49 CIGGf2127457 deregister reason lofi"
    stringOb6 match{
      case searchObj6(text,time,text2,hostName,text3,module,text4) => println(" match ob6")
      case _ => println("None")
    }
    val stringOb7 = "<134>0000152007: Jun  1 07:39:28: HNIP51601GC57: %ONTMNT-6-Informational: 11939729:83: 2017/06/01 07:39:28 ont 0/3/117 FPTT1690202f power off"
    stringOb7 match{
      case searchObj7(text,time,text2,hostName,text3,module,text4) => println(" match ob7")
      case _ => println("None")
    }
    val stringO3 = "<133>0000142260: Jun  1 21:28:47: HNIP31102GC57: %DEVICE-5-POWER-MANAGE: Power running no good detected, power NO : 1."
    stringO3 match{
      case searchObj3(text,time,text2,hostName,text3,module) => println("match ob3")
      case _ => println("None")
    }
    val string02 = "<133>0000160516: Jun  2 00:14:19: HNIP35201GC57: %OAM-5-CPU_BUSY: cpu is busy."
    string02 match{
      case searchObj2(text,time,text2,hostName,text3) => println("Match ob2")
      case _ => println("None")
    }

  }
}
