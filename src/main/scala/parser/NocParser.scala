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

  def extractValue(line: String): Option[AbtractLogLine]  ={
    line match{
      case searchObj(header,text,time,deviceName,text2,foundResult,text3) => Option(NocLogLineObject(foundResult,safetyParseStringToInt(header,-1),deviceName,time,getFacility(header),getSeverity(header)))
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