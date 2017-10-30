package parser

import java.text.{Format, SimpleDateFormat}
import java.util.Date

/** Python style. convert from Huyvt's code.
  * Poor you - whoever are mantaining this code.
  * Created by hungdv on 31/07/2017.
  */
class OpsviewParser extends AbtractLogParser{
  private val text = "(.*)"
  private val timeInLongFormat = "(\\d{10})"
  private val searchObj0 = s"\\[$timeInLongFormat\\] SERVICE ALERT: $text;$text;$text;$text;$text;$text".r

  private val opsviewBrasLookup = Predef.Map("HNI-MP02-1" -> "HNI-MP-02-01", "HCM-MP03-2-NEW" -> "HCM-MP-03-02",
    "VPC-MP01" -> "VPC-MP-01-01", "LGI-MP01-2" -> "LGI-MP-01-02", "DTP-MP01-2" -> "DTP-MP-01-02", "TQG-MP01-1" -> "TQG-MP-01-01",
    "HDG-MP01-3" -> "HDG-MP-01-03", "VLG-MP01-1" -> "VLG-MP-01-01", "AGG-MP01-1" -> "AGG-MP-01-01", "HYN-MP03" -> "HYN-MP-01-03",
    "DNI-MP01-2-NEW" -> "DNI-MP-01-02", "VTU-MP01-1-NEW" -> "VTU-MP-01-01", "DLK-MP01-2" -> "DLK-MP-01-02", "BDH-MP01-2" -> "BDH-MP-01-02",
    "QBH-MP01-2" -> "QBH-MP-01-02", "TNH-MP01-1" -> "TNH-MP-01-01", "BNH-MP01-3" -> "BNH-MP-01-03", "CMU-MP01-2" -> "CMU-MP-01-02",
    "THA-MP01-2" -> "THA-MP-01-02", "NAN-MP01-3" -> "NAN-MP-01-03", "DNG-MP01-2" -> "DNG-MP-01-02", "PYN-MP01-2" -> "PYN-MP-01-02",
    "HNI-MP05-2" -> "HNI-MP-05-02", "KGG-MP01-3" -> "KGG-MP01-3", "HNI-MP01-2" -> "HNI-MP-01-02", "LDG-MP01-2" -> "LDG-MP-01-02",
    "HNI-MP03-2" -> "HNI-MP-03-02", "HNI-MP02-3" -> "HNI-MP-Backup-02", "QNI-MP01-1" -> "QNI-MP-01-01", "QNH-MP03" -> "QNH-MP-01-03",
    "TNN-MP01-3" -> "TNN-MP-01-03", "HCM-MP05-1-NEW" -> "HCM-MP-05-01", "YBI-MP02" -> "YBI-MP-01-02", "DLK-MP01-4" -> "DLK-MP-01-04",
    "QNM-MP01-2" -> "QNM-MP-01-02", "BPC-MP01-1" -> "BPC-MP-01-01", "HNI-MP02-6" -> "HNI-MP-02-06", "GLI-MP01-2" -> "GLI-MP-01-02",
    "VTU-MP01-2-NEW" -> "VTU-MP-01-02", "LSN-MP02" -> "LSN-MP-01-02", "HCM-MP06-1" -> "HCM-MP-06-01", "QTI-MP01-1" -> "QTI-MP-01-01",
    "HCM-MP02-1" -> "HCM-MP-02-01", "BDH-MP01-4" -> "BDH-MP-01-04", "BDG-MP01-1-New" -> "BDG-MP-01-01", "HCM-MP04-1" -> "HCM-MP-04-01",
    "TGG-MP01-1-NEW" -> "TGG-MP-01-01", "LGI-MP01-1" -> "LGI-MP-01-01", "HCM-MP05-5" -> "HCM-MP-Backup-01", "DAH-MP02" -> "DAH-MP-01-02",
    "Hostname_Opsview" -> "Hostname_Noc", "DTP-MP01-1" -> "DTP-MP-01-01", "STY-MP02" -> "STY-MP-01-02", "LDG-MP01-4" -> "LDG-MP-01-04",
    "DLK-MP01-1" -> "DLK-MP-01-01", "BDH-MP01-1" -> "BDH-MP-01-01", "HTH-MP02" -> "HTH-MP-01-02", "QBH-MP01-1" -> "QBH-MP-01-01",
    "CTO-MP01-2-NEW" -> "CTO-MP01-2-NEW", "KTM-MP01-2" -> "KTM-MP-01-02", "HYN-MP02" -> "HYN-MP-01-02", "CMU-MP01-1" -> "CMU-MP-01-01",
    "BNH-MP01-2" -> "BNH-MP-01-02", "BRA-MP01-2" -> "BRA-MP-01-02", "HPG-MP01-NEW" -> "HPG-MP-01-01", "DNG-MP01-1" -> "DNG-MP-01-01",
    "TVH-MP01-2" -> "TVH-MP-01-02", "TGG-MP01-2-NEW" -> "TGG-MP-01-02", "BDG-MP01-2-New" -> "BDG-MP-01-02", "PTO-MP02" -> "PTO-MP-01-02",
    "HCM-MP01-2" -> "HCM-MP-01-02", "HNI-MP02-8" -> "HNI-MP-02-08", "HCM-MP05-2-NEW" -> "HCM-MP-05-02", "NTN-MP01-2" -> "NTN-MP-01-02",
    "THA-MP01-4" -> "THA-MP-01-04", "PYN-MP01-1" -> "PYN-MP-01-01", "NAN-MP01-2" -> "NAN-MP-01-02", "HDG-MP01-2" -> "HDG-MP-01-02",
    "QNH-MP02" -> "QNH-MP-01-02", "CTO-MP01-1" -> "CTO-MP01-1-NEW", "LDG-MP01-1" -> "LDG-MP-01-01", "TQG-MP01-2" -> "TQG-MP-01-02",
    "HNI-MP03-1" -> "HNI-MP-03-01", "BLC-MP01-2" -> "BLC-MP-01-02", "NTG-MP01-1-NEW" -> "NTG-MP-01-01", "HNI-MP01-1" -> "HNI-MP-01-01",
    "HNI-MP05-1" -> "HNI-MP-05-01", "TNN-MP01-2" -> "TNN-MP-01-02", "KGG-MP01-2" -> "KGG-MP-01-02", "GLI-MP01-1" -> "GLI-MP-01-01",
    "HYN-MP04" -> "HYN-MP-01-04", "HNI-MP02-5" -> "HNI-MP-02-05", "THA-MP01-1" -> "THA-MP-01-01", "BTE-MP01-2" -> "BTE-MP-01-02",
    "YBI-MP01" -> "YBI-MP-01-01", "DLK-MP01-3" -> "DLK-MP-01-03", "BDH-MP01-3" -> "BDH-MP-01-03", "HPG-MP02-NEW" -> "HPG-MP-01-02",
    "NAN-MP01-4" -> "NAN-MP-01-04", "GLI-MP01-4" -> "GLI-MP-01-04", "HCM-MP05-4" -> "HCM-MP-Backup-02", "DAH-MP01" -> "DAH-MP-01-01",
    "BTN-MP01-1-NEW" -> "BTN-MP-01-01", "STY-MP01" -> "STY-MP-01-01", "HDG-MP01-4" -> "HDG-MP-01-04", "QNH-MP04" -> "QNH-MP-01-04",
    "KTM-MP01-1" -> "KTM-MP-01-01", "TNN-MP01-4" -> "TNN-MP-01-04", "HNI-MP02-2" -> "HNI-MP-02-02", "HYN-MP01" -> "HYN-MP-01-01",
    "BRA-MP01-1" -> "BRA-MP-01-01", "KGG-MP01-4" -> "KGG-MP01-4", "QNI-MP01-2" -> "QNI-MP-01-02", "VPC-MP02" -> "VPC-MP-01-02",
    "VLG-MP01-2" -> "VLG-MP-01-02", "AGG-MP01-2" -> "AGG-MP-01-02", "LDG-MP01-3" -> "LDG-MP-01-03", "BNH-MP01-1" -> "BNH-MP-01-01",
    "HTH-MP01" -> "HTH-MP-01-01", "NTG-MP01-2-NEW" -> "NTG-MP-01-02", "HUE-MP01-1-NEW" -> "HUE-MP-01-01",
    "QNM-MP01-1" -> "QNM-MP-01-01", "LSN-MP01" -> "LSN-MP-01-01", "TNH-MP01-2" -> "TNH-MP-01-02",
    "HCM-MP01-1" -> "HCM-MP-01-01", "BNH-MP01-4" -> "BNH-MP-01-04", "QTI-MP01-2" -> "QTI-MP-01-02",
    "THA-MP01-3" -> "THA-MP-01-03", "PTO-MP01" -> "PTO-MP-01-01", "HNI-MP02-7" -> "HNI-MP-02-07",
    "NAN-MP01-1" -> "NAN-MP-01-01", "TVH-MP01-1" -> "TVH-MP-01-01", "HCM-MP03-1-NEW" -> "HCM-MP-03-01",
    "QNH-MP01" -> "QNH-MP-01-01", "BTN-MP01-2-NEW" -> "BTN-MP-01-02", "TNN-MP01-1" -> "TNN-MP-01-01",
    "KGG-MP01-1" -> "KGG-MP-01-01", "HDG-MP01-1" -> "HDG-MP-01-01", "BLC-MP01-1" -> "BLC-MP-01-01",
    "DNI-MP01-1-NEW" -> "DNI-MP-01-01", "BTE-MP01-1" -> "BTE-MP-01-01", "HUE-MP01-2-NEW" -> "HUE-MP-01-02",
    "HNI-MP02-4" -> "HNI-MP-Backup-01", "HCM-MP04-2" -> "HCM-MP-04-02",
    "NTN-MP01-1" -> "NTN-MP-01-01", "HCM-MP02-2" -> "HCM-MP-02-02", "BPC-MP01-2" -> "BPC-MP-01-02", "GLI-MP01-3" -> "GLI-MP-01-03", "HCM-MP06-2" -> "HCM-MP-06-02")
  private val opsviewBrasLookupNew = Predef.Map("GLI-MP-01-01" -> "GLI-MP-01-01", "DLK-MP-01-03" -> "DLK-MP-01-03",
    "BPC-MP-01-01" -> "BPC-MP-01-01", "QNH-MP-01-03" -> "QNH-MP-01-03", "HCM-MP-02-02" -> "HCM-MP-02-02",
    "HCM-MP-01-01" -> "HCM-MP-01-01", "AGG-MP-01-02" -> "AGG-MP-01-02", "LDG-MP-01-01" -> "LDG-MP-01-01",
    "HYN-MP-01-03" -> "HYN-MP-01-03", "QBH-MP-01-02" -> "QBH-MP-01-02", "TNN-MP-01-02" -> "TNN-MP-01-02",
    "HCM-MP-Backup-01" -> "HCM-MP-Backup-01", "HNI-MP-01-01" -> "HNI-MP-01-01", "VTU-MP-01-01" -> "VTU-MP-01-01",
    "HCM-MP-05-01" -> "HCM-MP-05-01", "LDG-MP-01-04" -> "LDG-MP-01-04", "HCM-MP-06-02" -> "HCM-MP-06-02",
    "BDG-MP-01-02" -> "BDG-MP-01-02", "YBI-MP-01-02" -> "YBI-MP-01-02", "BTN-MP-01-02" -> "BTN-MP-01-02",
    "LSN-MP-01-02" -> "LSN-MP-01-02", "STY-MP-01-01" -> "STY-MP-01-01", "KGG-MP-01-01" -> "KGG-MP-01-01",
    "TVH-MP-01-01" -> "TVH-MP-01-01", "NAN-MP-01-04" -> "NAN-MP-01-04", "QNM-MP-01-02" -> "QNM-MP-01-02",
    "NTG-MP-01-02" -> "NTG-MP-01-02", "HNI-MP-Backup-01" -> "HNI-MP-Backup-01", "HNI-MP-05-01" -> "HNI-MP-05-01",
    "HNI-MP-02-07" -> "HNI-MP-02-07", "DNI-MP-01-02" -> "DNI-MP-01-02", "KGG-MP01-3" -> "KGG-MP01-3",
    "DTP-MP-01-02" -> "DTP-MP-01-02", "HUE-MP-01-02" -> "HUE-MP-01-02", "GLI-MP-01-03" -> "GLI-MP-01-03",
    "BDH-MP-01-02" -> "BDH-MP-01-02", "HNI-MP-03-02" -> "HNI-MP-03-02", "QTI-MP-01-02" -> "QTI-MP-01-02",
    "HTH-MP-01-02" -> "HTH-MP-01-02", "DAH-MP-01-02" -> "DAH-MP-01-02", "CTO-MP01-1-NEW" -> "CTO-MP01-1-NEW",
    "VLG-MP-01-02" -> "VLG-MP-01-02", "BLC-MP-01-02" -> "BLC-MP-01-02", "DNG-MP-01-01" -> "DNG-MP-01-01",
    "VPC-MP-01-01" -> "VPC-MP-01-01", "Hostname_Noc" -> "Hostname_Noc", "TNN-MP-01-04" -> "TNN-MP-01-04",
    "THA-MP-01-01" -> "THA-MP-01-01", "TNH-MP-01-01" -> "TNH-MP-01-01", "PYN-MP-01-02" -> "PYN-MP-01-02",
    "NAN-MP-01-01" -> "NAN-MP-01-01", "CMU-MP-01-01" -> "CMU-MP-01-01", "QNH-MP-01-02" -> "QNH-MP-01-02",
    "LGI-MP-01-01" -> "LGI-MP-01-01", "QBH-MP-01-01" -> "QBH-MP-01-01", "THA-MP-01-04" -> "THA-MP-01-04",
    "HYN-MP-01-02" -> "HYN-MP-01-02", "AGG-MP-01-01" -> "AGG-MP-01-01", "HCM-MP-03-02" -> "HCM-MP-03-02",
    "HCM-MP-02-01" -> "HCM-MP-02-01", "HDG-MP-01-02" -> "HDG-MP-01-02", "DLK-MP-01-02" -> "DLK-MP-01-02",
    "NTN-MP-01-02" -> "NTN-MP-01-02", "HCM-MP-06-01" -> "HCM-MP-06-01", "PTO-MP-01-02" -> "PTO-MP-01-02",
    "HNI-MP-02-01" -> "HNI-MP-02-01", "CTO-MP01-2-NEW" -> "CTO-MP01-2-NEW", "BDG-MP-01-01" -> "BDG-MP-01-01",
    "YBI-MP-01-01" -> "YBI-MP-01-01", "BTN-MP-01-01" -> "BTN-MP-01-01", "BTE-MP-01-02" -> "BTE-MP-01-02",
    "BNH-MP-01-02" -> "BNH-MP-01-02", "TNN-MP-01-01" -> "TNN-MP-01-01", "LDG-MP-01-03" -> "LDG-MP-01-03",
    "NAN-MP-01-03" -> "NAN-MP-01-03", "HCM-MP-04-02" -> "HCM-MP-04-02", "QNI-MP-01-02" -> "QNI-MP-01-02",
    "HNI-MP-02-06" -> "HNI-MP-02-06", "LSN-MP-01-01" -> "LSN-MP-01-01", "QTI-MP-01-01" -> "QTI-MP-01-01",
    "HNI-MP-03-01" -> "HNI-MP-03-01", "TQG-MP-01-02" -> "TQG-MP-01-02", "HTH-MP-01-01" -> "HTH-MP-01-01",
    "QNH-MP-01-04" -> "QNH-MP-01-04", "HDG-MP-01-04" -> "HDG-MP-01-04", "VLG-MP-01-01" -> "VLG-MP-01-01",
    "TGG-MP-01-02" -> "TGG-MP-01-02", "HYN-MP-01-04" -> "HYN-MP-01-04", "DTP-MP-01-01" -> "DTP-MP-01-01",
    "DAH-MP-01-01" -> "DAH-MP-01-01", "TNN-MP-01-03" -> "TNN-MP-01-03", "BDH-MP-01-04" -> "BDH-MP-01-04",
    "PYN-MP-01-01" -> "PYN-MP-01-01", "BLC-MP-01-01" -> "BLC-MP-01-01", "VTU-MP-01-02" -> "VTU-MP-01-02",
    "BNH-MP-01-04" -> "BNH-MP-01-04", "BRA-MP-01-02" -> "BRA-MP-01-02", "THA-MP-01-03" -> "THA-MP-01-03",
    "QNM-MP-01-01" -> "QNM-MP-01-01", "HYN-MP-01-01" -> "HYN-MP-01-01", "QNH-MP-01-01" -> "QNH-MP-01-01",
    "NTG-MP-01-01" -> "NTG-MP-01-01", "HCM-MP-03-01" -> "HCM-MP-03-01", "HPG-MP-01-02" -> "HPG-MP-01-02",
    "KGG-MP-01-02" -> "KGG-MP-01-02", "HDG-MP-01-01" -> "HDG-MP-01-01", "KTM-MP-01-02" -> "KTM-MP-01-02",
    "HUE-MP-01-01" -> "HUE-MP-01-01", "PTO-MP-01-01" -> "PTO-MP-01-01", "GLI-MP-01-02" -> "GLI-MP-01-02",
    "HCM-MP-01-02" -> "HCM-MP-01-02", "DNI-MP-01-01" -> "DNI-MP-01-01", "DLK-MP-01-04" -> "DLK-MP-01-04",
    "BNH-MP-01-01" -> "BNH-MP-01-01", "BDH-MP-01-01" -> "BDH-MP-01-01", "LDG-MP-01-02" -> "LDG-MP-01-02",
    "KGG-MP01-4" -> "KGG-MP01-4", "BPC-MP-01-02" -> "BPC-MP-01-02", "HNI-MP-01-02" -> "HNI-MP-01-02",
    "HCM-MP-05-02" -> "HCM-MP-05-02", "HCM-MP-Backup-02" -> "HCM-MP-Backup-02", "QNI-MP-01-01" -> "QNI-MP-01-01",
    "HNI-MP-02-08" -> "HNI-MP-02-08", "LGI-MP-01-02" -> "LGI-MP-01-02", "STY-MP-01-02" -> "STY-MP-01-02",
    "HNI-MP-Backup-02" -> "HNI-MP-Backup-02", "TVH-MP-01-02" -> "TVH-MP-01-02", "HNI-MP-05-02" -> "HNI-MP-05-02",
    "TQG-MP-01-01" -> "TQG-MP-01-01", "TGG-MP-01-01" -> "TGG-MP-01-01", "DLK-MP-01-01" -> "DLK-MP-01-01",
    "HDG-MP-01-03" -> "HDG-MP-01-03", "BNH-MP-01-03" -> "BNH-MP-01-03", "BDH-MP-01-03" -> "BDH-MP-01-03",
    "BTE-MP-01-01" -> "BTE-MP-01-01", "NTN-MP-01-01" -> "NTN-MP-01-01", "GLI-MP-01-04" -> "GLI-MP-01-04",
    "BRA-MP-01-01" -> "BRA-MP-01-01", "HNI-MP-02-02" -> "HNI-MP-02-02", "CMU-MP-01-02" -> "CMU-MP-01-02",
    "HCM-MP-04-01" -> "HCM-MP-04-01", "NAN-MP-01-02" -> "NAN-MP-01-02", "HNI-MP-02-05" -> "HNI-MP-02-05",
    "KTM-MP-01-01" -> "KTM-MP-01-01", "VPC-MP-01-02" -> "VPC-MP-01-02", "HPG-MP-01-01" -> "HPG-MP-01-01",
    "THA-MP-01-02" -> "THA-MP-01-02", "TNH-MP-01-02" -> "TNH-MP-01-02", "DNG-MP-01-02" -> "DNG-MP-01-02")


  def extracValues(line: String) : Option[OpsviewLogLineObject] ={
    line match{
      case searchObj0(time,text0,text1,text2,text3,text4,text5) => Option(OpsviewLogLineObject("SERVICE ALERT",
        getTime(time),opsviewBrasLookupNew.getOrElse(text0,"n/a"),text1.split(":")(0),text2,text3,text4,text5.replace(",","")))
      case _ =>  None
    }
  }

  /**
    * Dont need to map bras-name format
    * @param line
    * @return
    */
  def extracValuesWithoutMapping(line: String) : Option[OpsviewLogLineObject] ={
    line match{
      case searchObj0(time,text0,text1,text2,text3,text4,text5) => Option(OpsviewLogLineObject("SERVICE ALERT",
        getTime(time),text0,text1.split(":")(0),text2,text3,text4,text5.replace(",","")))
      case _ =>  None
    }
  }

  def getTime(timeInMilis: String): String ={
    val timeLong = safetyParserStringToLong(timeInMilis,0)
    //println("timeLong: " + timeLong)
    val date: Date = new Date(timeLong*1000)
    val format: Format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.format(date)
  }
  def safetyParserStringToLong(aString:String,returnValue:Long) ={
    toLong(aString) match{
      case Some(n) => n
      case None => returnValue
    }
  }
  def toLong(s:String): Option[Long]={
    try{
      Some(s.toLong)
    }
    catch {
      case e:NumberFormatException => None
    }
  }

}
object ParseOpsviewTest{
  def main (args: Array[String] ): Unit = {
    val op1 = "[1501489197] EXTERNAL COMMAND: ACKNOWLEDGE_SVC_PROBLEM;INF-THA-THAP00601GC57-20.221.1.29;check_fts_pon_port_status_api;1;1;0;phapvm;down do cup dien"
    val op2 = "[1501489197] SERVICE ALERT: INF-HN9-HNIP48801DF24-20.14.8.26;check_sw_cpu_dasan;OK;HARD;2;OK: CPU 5%"
    val op3 = "[1501489197] SERVICE ALERT: INF-HPG-HPGP02302GC57-10.10.219.124;Check_olt_cpu;OK;HARD;1;OK: CPU Idle 79%"
    val op4 = "[1501489197] SERVICE ALERT: Test-HNIM00201HW12-10.10.83.20;check_interface_huawei_new: 31;OK;SOFT;2;OK: 10GE3/0/21 DWL-CE102002HNIP29701HW24-X1/0/1, UP, throughput (in/out) 78.36/2372.28 Mbps, speed 10000"
    val op5 = "[1504130786] SERVICE ALERT: Pon-alert;Pon-alert: LinkDown;CRITICAL;HARD;3;CRITICAL: message:(\"LinkDown\")2017-08-31T05:01:55.163000Z- HPGP07302GC57(20.101.25.2)-  <131>0000124197: Aug 31 04:59:40: HPGP07302GC57: %DEVICE-3-LINKUPDOWN: p0/8 LinkDown."
    val parser = new OpsviewParser
    println(parser.extracValues(op1.trim))
    println(parser.extracValues(op2.trim))
    println(parser.extracValues(op3.trim))
    println(parser.extracValues(op5.trim))

     val text = "(.*)"
     //val text = "[([a-zA-Z0-9]+)]"
     val timeInLongFormat = "(\\d{10})"
    val searchObj0 = s"\\[$timeInLongFormat\\] SERVICE ALERT: $text;$text;$text;$text;$text;$text".r
    val searchObj1 = s"$timeInLongFormat SERVICE ALERT: $text".r
    op3 match {
      case searchObj0(time,text0,text1,text2,text3,text4,text5) => println("true0")
      case searchObj1(time,text) => println("true1")
      case _ => println("false")
    }
    val string = "1501489197"
    val time = parser.getTime(string)
    println(time)
 }
}

case class OpsviewLogLineObject(
                               found_result: String,
                               date_time: String,
                               host_name: String,
                               service_name: String,
                               service_status: String,
                               alert_state: String,
                               alert_code: String,
                               messge: String
                               ) extends Serializable
