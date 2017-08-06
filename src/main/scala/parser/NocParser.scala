package parser

/**
  * Created by hungdv on 20/06/2017.
  */
class NocParser extends AbtractLogParser{
  private val text =  "(.*)"

  val time = "(\\d{2}:\\d{2}:\\d{2})"
  //  val date = "(\\w{3,}  \\d{1,})"
  val date = "(\\w{3,}\\s{1,}\\d{1,})"

  val header = "<([0-9]+)>"

  val deviceName = "([A-Z][A-Z0-9.-]+)"

  val foundResult = "([A-Z0-9-_]+)"

  val sev = Predef.Map(0 ->"emerg",
                        1 ->"alert",
                        2 ->"crit",
                        3 ->"err",
                        4 ->"warning",
                        5 ->"notice",
                        6 ->"info",
                        7 ->"debug")
  val fac = Predef.Map(0 ->    "kern",
                        1 ->    "user",
                        2 ->    "mail",
                        3 ->    "daemon",
                        4 ->    "auth",
                        5 ->    "syslog",
                        6 ->    "lpr",
                        7 ->    "news",
                        8 ->    "uucp",
                        9 ->    "clock daemon",
                        10 ->    "authpriv",
                        11 ->    "ftp",
                        12 ->    "NTP subsystem",
                        13 ->    "log audit",
                        14 ->    "log alert",
                        15 ->    "cron",
                        16 ->    "local0",
                        17 ->    "local1",
                        18 ->    "local2",
                        19 ->    "local3",
                        20 ->    "local4",
                        21 ->    "local5",
                        22 ->    "local6",
                        23 ->    "local7")

  val searchObj = s"$header$text $time $deviceName $text %$foundResult: $text".r


  val nocBrasMapping = Predef.Map("GLI-MP-01-01" -> "GLI-MP-01-01", "DLK-MP-01-03" -> "DLK-MP-01-03",
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
    "PYN-MP-01-01" -> "PYN-MP-01-01", "BLC-MP-01-01" -> "BLC-MP-01-01", "VTU-MP-01-02" -> "VTU-MP-01-02", "BNH-MP-01-04" -> "BNH-MP-01-04", "BRA-MP-01-02" -> "BRA-MP-01-02",
    "THA-MP-01-03" -> "THA-MP-01-03", "QNM-MP-01-01" -> "QNM-MP-01-01", "HYN-MP-01-01" -> "HYN-MP-01-01", "QNH-MP-01-01" -> "QNH-MP-01-01", "NTG-MP-01-01" -> "NTG-MP-01-01",
    "HCM-MP-03-01" -> "HCM-MP-03-01", "HPG-MP-01-02" -> "HPG-MP-01-02", "KGG-MP-01-02" -> "KGG-MP-01-02", "HDG-MP-01-01" -> "HDG-MP-01-01", "KTM-MP-01-02" -> "KTM-MP-01-02",
    "HUE-MP-01-01" -> "HUE-MP-01-01", "PTO-MP-01-01" -> "PTO-MP-01-01", "GLI-MP-01-02" -> "GLI-MP-01-02", "HCM-MP-01-02" -> "HCM-MP-01-02", "DNI-MP-01-01" -> "DNI-MP-01-01",
    "DLK-MP-01-04" -> "DLK-MP-01-04", "BNH-MP-01-01" -> "BNH-MP-01-01", "BDH-MP-01-01" -> "BDH-MP-01-01", "LDG-MP-01-02" -> "LDG-MP-01-02", "KGG-MP01-4" -> "KGG-MP01-4",
    "BPC-MP-01-02" -> "BPC-MP-01-02", "HNI-MP-01-02" -> "HNI-MP-01-02", "HCM-MP-05-02" -> "HCM-MP-05-02", "HCM-MP-Backup-02" -> "HCM-MP-Backup-02", "QNI-MP-01-01" -> "QNI-MP-01-01",
    "HNI-MP-02-08" -> "HNI-MP-02-08", "LGI-MP-01-02" -> "LGI-MP-01-02", "STY-MP-01-02" -> "STY-MP-01-02", "HNI-MP-Backup-02" -> "HNI-MP-Backup-02", "TVH-MP-01-02" -> "TVH-MP-01-02",
    "HNI-MP-05-02" -> "HNI-MP-05-02", "TQG-MP-01-01" -> "TQG-MP-01-01", "TGG-MP-01-01" -> "TGG-MP-01-01", "DLK-MP-01-01" -> "DLK-MP-01-01", "HDG-MP-01-03" -> "HDG-MP-01-03",
    "BNH-MP-01-03" -> "BNH-MP-01-03", "BDH-MP-01-03" -> "BDH-MP-01-03", "BTE-MP-01-01" -> "BTE-MP-01-01", "NTN-MP-01-01" -> "NTN-MP-01-01", "GLI-MP-01-04" -> "GLI-MP-01-04",
    "BRA-MP-01-01" -> "BRA-MP-01-01", "HNI-MP-02-02" -> "HNI-MP-02-02", "CMU-MP-01-02" -> "CMU-MP-01-02", "HCM-MP-04-01" -> "HCM-MP-04-01", "NAN-MP-01-02" -> "NAN-MP-01-02",
    "HNI-MP-02-05" -> "HNI-MP-02-05", "KTM-MP-01-01" -> "KTM-MP-01-01", "VPC-MP-01-02" -> "VPC-MP-01-02", "HPG-MP-01-01" -> "HPG-MP-01-01", "THA-MP-01-02" -> "THA-MP-01-02",
    "TNH-MP-01-02" -> "TNH-MP-01-02", "DNG-MP-01-02" -> "DNG-MP-01-02")

  def extractValue(line: String): Option[AbtractLogLine]  ={
    line match{
      case searchObj(header,text,time,deviceName,text2,foundResult,text3) => Option(NocLogLineObject(foundResult,safetyParseStringToInt(header,-1),nocBrasMapping.getOrElse(deviceName,"n/a"),time,getFacility(header),getSeverity(header)))
      case _ => None
    }
  }
  def getFacility(header: String): String ={
    val p = safetyParseStringToInt(header,0)
    val faci = scala.math.floor(p/8).toInt
    val facility = fac.getOrElse(faci,"n/a")
    facility
  }

  def getSeverity(header: String): String ={
    val p = safetyParseStringToInt(header,0)
    val sevi = p%8
    val severity = sev.getOrElse(sevi,"n/a")
    severity
  }
  def safetyParseStringToInt(s:String,valueReturnedWhenNone:Int):Int ={
    toInt(s) match{
      case Some(i) => i
      case None => valueReturnedWhenNone
    }
  }
  private def toInt(s: String): Option[Int] ={
    try { Some(s.toInt)} catch{
      case e:NumberFormatException => None
    }
  }

}

case class NocLogLineObject(val error: String,val pri: Int,val devide: String, val time: String,val facility: String,val severity: String) extends AbtractLogLine


object NocParserTest{
  def main(args: Array[String]): Unit = {
    val strings = List("<190>Jul  1 07:00:30 NTN-MP01-2 file[52402]: %INTERACT-6-UI_CMDLINE_READ_LINE: User 'noctool', command 'command rpc rpc command show chassis fpc '",
    "<28>Jul  1 07:00:30 NTN-MP01-2 jddosd[1662]: %DAEMON-4-DDOS_PROTOCOL_VIOLATION_SET: Protocol SSH:aggregate is violated at routing-engine for 59 times, started at 2017-07-01 07:00:30 ICT",
    "<188>6440778: *Oct 21 20:04:23.818: %SW_DAI-4-DHCP_SNOOPING_DENY: 1 Invalid ARPs (Res) on Gi1/0/8, vlan 40.([90b1.1c0e.6e75/192.168.0.120/0000.0000.0000/192.168.0.120/20:04:22 UTC Fri Oct 21 1994])",
    "<164>Jul  1 07:00:30 VTU-MP01-1-NEW : %PFE-4: fpc1 dfw_msg_service_counter_get: Valid Filter Index 404446, packet is NULL, counter name __junos-dyn-service-counter not found.")
    val parser = new NocParser
    strings.foreach{
      string =>
        val parsed  = parser.extractValue(string)
        println(parsed)
    }
  }
}