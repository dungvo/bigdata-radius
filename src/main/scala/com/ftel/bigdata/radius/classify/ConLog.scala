package com.ftel.bigdata.radius.classify

import com.ftel.bigdata.radius.utils.BrasUtil
import com.ftel.bigdata.utils.Parameters.TAB
import com.ftel.bigdata.utils.DateTimeUtil
import org.joda.time.DateTime
import com.ftel.bigdata.utils.Parameters

/**
 * 07:02:23 000005B8 Acct-Local:LogOff: Hndsl-131014-079, HN-MP02-8, xe-3/1/1.3105:3105#HNIP48702GC57 PON 0/1/103 4cf2bf8db466 3105 CIGGe3926089, C701162
    CIGGe3926089: mã serial ONU
    Hndsl-131014-079: số định danh của KHG
    3105: mã vlan
    Acct-Local:LogOff: loại kết nối, tức là LogOff
    000005B8: mã session
    0/1/103: 0/mã ONT/mã index
    4cf2bf8db466: địa chỉ MAC của KHG
    HNIP48702GC57: mã OLT
    HN-MP02-8: mã BRAS
    3/1/1: mã line card/ mã card/ mã port
 */
case class ConLog(
    timestamp: Long,
    session: String,
    typeLog: String,
    name: String,
    nasName: String,
    card: Card,     // xe-3/1/1.3105:3105#HNIP48702GC57
    cable: Cable,   // PON 0/1/103
    mac: String,
    vlan: Int,
    serialONU: String,
    text: String) extends AbstractLog {
  
  def this(timestamp: Long, session: String, typeLog: String, name: String, nasName: String, text: String) = this(timestamp, session, typeLog, name, nasName, new Card(), new Cable(), null, 0, null, text)
  override def toString = Array(
      timestamp,
      session,
      typeLog,
      name,
      nasName,
      card.toString(),
      cable.toString(),
      mac,
      vlan,
      serialONU,
      text).mkString(TAB)
 
      
  //override def toString = "CONLOG-TEST-STREAMING: "
  override def get() = "con/" + typeLog.toLowerCase()
  override def getKey() = {
    val date = DateTimeUtil.create(timestamp / 1000L)
    "day=" + date.toString("yyyy-MM-dd") +
    "/type=" + get() +
    "/hour=" + date.toString("HH") +
    "/" + date.toString("mm")
  }
  override def getTimestamp(): Long = timestamp
  override def toES = Map(
      "type" -> "con",
      "timestamp" -> DateTimeUtil.create(timestamp / 1000L).toString(Parameters.ES_5_DATETIME_FORMAT),
      "session" -> session,
      "typeLog" -> typeLog,
      "name" -> name,
      "nasName" -> nasName,
      "card.id" -> card.id,
      "card.lineId" -> card.lineId,
      "card.port" -> card.port,
      "card.vlan" -> card.vlan,
      "card.olt" -> card.olt,
      "cable.number" -> cable.number,
      "cable.ontId" -> cable.ontId,
      "cable.indexId" -> cable.indexId,
      "mac" -> mac,
      "vlan" -> vlan,
      "serialONU" -> serialONU,
      "text" -> text)
}

/**
 * xe-3/1/1.3105:3105#HNIP48702GC57
 */
case class Card(
    lineId: Int,
    id: Int,
    port: Int,
    vlan: Int,
    olt: String) {
  def this() = this(-1, -1, -1, -1, "N/A")
  def this(lineId: Int, id: Int, port: Int, olt: String) = this(lineId, id, port, -1, olt)
  override def toString = Array(lineId, id, port, olt).mkString(TAB)
}

/**
 * xe-1/1/1.3619:3619#HNIP51101GC57
 */
object Card {
  def apply(text: String): Card = {
    val regex = s"xe-(\\d)/(\\d)/(\\d).(\\d{4}):\\d{4}#(\\w*)".r("lineId", "id", "port", "vlan", "olt")
    text match {
      case regex(lineId, id, port, vlan, olt) => Card(lineId.toInt, id.toInt, port.toInt, vlan.toInt, olt)
      case _ => new Card()
    }
  }
}

/**
 * PON 0/1/103
 */
case class Cable(
    number: Int,
    ontId: Int,
    indexId: Int) {
  def this() = this(-1, -1, -1)
  def this(arr: Array[String]) = this(arr(0).toInt, arr(1).toInt, arr(2).toInt)
  override def toString = Array(number, ontId, indexId).mkString(TAB)
}

    
object ConLog {
  
  // 08:03:22 00001474 Acct-Local:LogOff: tnfdl-151217-842, TNN-MP02, xe-1/1/0.3206:3206#TNNP03101GC56 PON 0/5/11  3206, 904FE611
//  private val TIME = "(\\d{2}:\\d{2}:\\d{2})"
//  private val SESSION = "(\\w{8})"
//  private val TYPE = "Acct-Local:(\\w{6})"
//  private val BRAS_NAME = "(\\s+\\w+-\\w+-\\w+)"
//  private val CONTRACT_NAME = "(\\w+-\\w+-\\w+-\\w+|\\w+-\\w+-\\w+|\\w+-\\w+)"
//  private val CARD = "(\\d\\/\\d\\/\\d)"
//  private val VLAN = "(\\d{4})"
//  private val OLT = "(\\w+)"
//  private val CABLE = "(\\d+\\/\\d+\\/\\d+)"
//  
//  private val BASIC = s"${TIME}\\s+${SESSION}\\s+${TYPE}:${BRAS_NAME}"
//  val REGEX01 = s"${BASIC},\\s+${CONTRACT_NAME},\\s+xe-${CARD}.${VLAN}:${VLAN}#${OLT}\\s+\\w{3}\\s+${CABLE}\\s+${VLAN},\\s\\w+".r
//  
//  //11:25:46 0000051C Auth-Local:SignIn: Hnfdl-160524-065, HN-MP01-1, xe-0/0/1.3001:3001#GPON PON 0/1 3001, F84693F7
//  val REGEX02 = s"${BASIC},\\s+${CONTRACT_NAME},\\s+xe-${CARD}.${VLAN}:${VLAN}#${OLT}\\s+\\w{3}\\s+\\d+\\/\\d+\\s+${VLAN},\\s\\w+".r
  
  
  
  /**
   * Some format wrong such as:
   *  - Reject
   *  - 07:00:00,00000A64,Acct-Local:LogOff,Ctfdl-160528-397,CTO-MP01-1-NEW,77BACF41
   *  - 08:03:22,00006140,Acct-Local:LogOff,ktdsl-140303-967,GLI-MP01-1,xe-1/3/0.1050:1050#KTMP01201ES52 atm 1/56:0.33:1050, 1000041A
   *  - 08:03:23,000017C4,Acct-Local:LogOff,Dndsl-140814-648,DNI-MP01-1-NEW,xe-5/1/1.1285:1285#DNIP00501GC56 PON 0/6/45  1285, D703731A
   *  - 08:03:23,0000A1F4,Acct-Local:LogOff,Ppdsl-150312-272,MX480-03,EF1D8D59
   * Những log này là log của những thiết bị mà mình chưa quan tâm tới, thiết bị đó nó không chứa đủ thông tin log.
   */
  private val SIZE_LENGTH_COMMA = 3
  
  private val SIGNIN = "Auth-Local:SignIn:" -> "SignIn"
  private val LOGOFF = "Acct-Local:LogOff:" -> "LogOff"
  private val REJECT = "Auth-Local:Reject:" -> "Reject"
  
  def apply(line: String, timestamp: Long): AbstractLog = {
    
    val arr = line.split(",")
    val size = arr.size
    //println("===============> " + line + "[" + day + "] [" + Parser.DATE_TIME_PATTERN_NOMALIZE + s"] [$size]")
    if (size >= SIZE_LENGTH_COMMA) {
      val arrOne = arr(0).split(" ")
      val day = DateTimeUtil.create(timestamp / 1000).toString(DateTimeUtil.YMD)
      val newTimestamp: Long = DateTimeUtil.create(day + " " + arrOne(0), Parser.DATE_TIME_PATTERN_NOMALIZE).getMillis()
      val session: String = arrOne(1)
      val typeLog: String = getType(arrOne(2))
      val name: String = arrOne.slice(3, arrOne.length).mkString
      val nasName: String = BrasUtil.getHostnameNoc(arr(1))
      val text: String = arr(2)//.slice(1, 3).mkString("\t").trim()
      val remain: String = arr.slice(3, size).mkString(",").trim()
//      println(s"[$line] ARR(0)===> " + arr(0))
//      println(s"[$line] date===> " + date)
//      println(s"[$line] session===> " + session)
//      println(s"[$line] typeLog===> " + typeLog)
//      println(s"[$line] name===> " + name)
//      println(s"[$line] nasName===> " + nasName)
//      println(s"[$line] text===> " + text)
//      println(s"[$line] remain===> " + remain)
      if (typeLog != null) {
        if (typeLog == REJECT._2) new ConLog(newTimestamp, session, typeLog, name, nasName, text + "," + remain)
        else if (typeLog == SIGNIN._2 || typeLog == LOGOFF._2) {
          try {
            val con = getConLog(newTimestamp, session, typeLog, name, nasName, text, remain)
            //println("CON: " + con)
            con
          } catch {
            case e: Exception => println("Line: " + line); ErrLog(timestamp, line)
          }
        } else new ErrLog(timestamp, line)
      } else new ErrLog(timestamp, line)
    } else new ErrLog(timestamp, line)
  }

  private def getConLog(timestamp: Long, session: String, typeLog: String, name: String, nasName: String, text: String, remain: String): ConLog = {
    //println(text)
    val arr = text.trim().split("\\s+")
    val size = arr.size
    size match {
      // HNI-MP-06-01, xe-1/1/1.3619:3619#HNIP51101GC57 PON 0/1/95 a85840286756 3619 FPTT15c000c1
      case 6 => {
        //val nasName: String = BrasUtil.getHostnameNoc(arr(0))
        val card = Card(arr(0).trim())
        val cableArray = arr(2).split("/")
        val cable = if (cableArray.size == 3) new Cable(cableArray) else new Cable()
        val mac = arr(3)
        val vlan = card.vlan//arr(5).toInt
        val serialONU: String = arr(5)
//        val textRemain: String = arr(7)
        //println("8 => " + text)
        ConLog(timestamp, session, typeLog, name, nasName, card, cable, mac, vlan, serialONU, remain)
      }
      case 4 => {
        val card = Card(arr(0).trim())
        val cableArray = arr(2).split("/")
        val cable = if (cableArray.size == 3) new Cable(cableArray) else new Cable()
        val mac = null //arr(4)
        val vlan = card.vlan//BrasUtil.trim(arr(4), ",").toInt
        val serialONU: String = null
        //val textRemain: String = null
        //println("5 => " + text)
        ConLog(timestamp, session, typeLog, name, nasName, card, cable, mac, vlan, serialONU, remain)
      }
      case 5 => {
        val nasName: String = BrasUtil.getHostnameNoc(arr(0))
        val card = Card(arr(1).trim())
        val cable = new Cable()
        val mac = null //arr(4)
        val vlan = card.vlan
        val serialONU: String = null
        val textRemain: String = null
        //println("5 => " + text)
        ConLog(timestamp, session, typeLog, name, nasName, card, cable, mac, vlan, serialONU, textRemain)
      }
      case 3 => {
        val nasName: String = BrasUtil.getHostnameNoc(arr(0))
        val card = Card(arr(1).trim())
        val cable = new Cable() //new Cable(arr(3).split("/"))
        val mac = null //arr(4)
        val vlan = card.vlan
        val serialONU: String = null
        val textRemain: String = null
        //println("5 => " + text)
        ConLog(timestamp, session, typeLog, name, nasName, card, cable, mac, vlan, serialONU, textRemain)
      }
      case 1 => {
        if (arr(0).trim().startsWith("xe-")) {
          val card = Card(arr(0).trim())
          ConLog(timestamp, session, typeLog, name, nasName, card, new Cable(), null, card.vlan, null, null)
        } else new ConLog(timestamp, session, typeLog, name, nasName, text + "," + remain)
      }
      case _ => {
        //println("TEXT => " + text + s"[Size: $size]")
        new ConLog(timestamp, session, typeLog, name, nasName, text + "," + remain)
      }
    }
  }
  
  private def getType(typeLog: String): String = {
    if (typeLog == SIGNIN._1) SIGNIN._2
    else if (typeLog == LOGOFF._1) LOGOFF._2
    else if (typeLog == REJECT._1) REJECT._2
    else null
  }
  
  def apply(line: String): ConLog = {
    val arr = line.split(TAB)
    val i = new AutoIncrease(-1)
    try {
    ConLog(
      //DateTimeUtil.create(arr(i.get), Parser.DATE_TIME_PATTERN_NOMALIZE),
      arr(i.get).toLong,
      arr(i.get),
      arr(i.get),
      arr(i.get),
      arr(i.get),
      new Card(arr(i.get).toInt, arr(i.get).toInt, arr(i.get).toInt, arr(i.get)),
      Cable(arr(i.get).toInt, arr(i.get).toInt, arr(i.get).toInt),
      arr(i.get),
      arr(i.get).toInt,
      arr(i.get),
      arr(i.get))
    } catch {
      case e: Exception => {
        println("WRONG: " + line);
        null
      }
      
    }
  }
  
  def main(args: Array[String]) {
//    08:59:14 0000175C Acct-Local:LogOff: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-5/1/1.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49
//08:59:27 00000A8C Acct-Local:LogOff: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-4/3/0.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49
//09:05:50 00000834 Acct-Local:LogOff: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-4/3/0.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49
//09:05:52 00001264 Auth-Local:SignIn: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-4/3/0.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49
//09:06:03 00000BD8 Auth-Local:SignIn: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-4/1/0.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49
//09:07:53 0000671C Acct-Local:LogOff: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-4/3/0.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49
//09:11:49 00001088 Acct-Local:LogOff: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-4/1/0.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49
//09:39:24 000013B4 Auth-Local:SignIn: Bedsl-160927-319, BTE-MP01-1, xe-0/1/0.1055:1055#BTEP00201ES60 atm 15/30:0.33:1055, 5282DD83, Limit 110 minutes
//11:23:20 00000658 Acct-Local:LogOff: Hnfdl-160524-065, HN-MP01-1, xe-0/1/1.3001:3001#GPON PON 0/1 3001, F84693F7
//11:25:46 0000051C Auth-Local:SignIn: Hnfdl-160524-065, HN-MP01-1, xe-0/0/1.3001:3001#GPON PON 0/1 3001, F84693F7

    //val line = "06:59:59 0000070C Auth-Local:SignIn: Thdsl-130731-568, THA-MP01-2, xe-0/3/0.3903:3903#THAP06301GC57 PON 0/5/19 9c50eedefc8e 3903 FPTT1750fd6d, 8CB116DB"
    //val line = "11:25:46 0000051C Auth-Local:SignIn: Hnfdl-160524-065, HN-MP01-1, xe-0/0/1.3001:3001#GPON PON 0/1 3001, F84693F7"
//    val line = "08:59:27 00000A8C Acct-Local:LogOff: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-4/3/0.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49"
    //val line = "09:39:24 000013B4 Auth-Local:SignIn: Bedsl-160927-319, BTE-MP01-1, xe-0/1/0.1055:1055#BTEP00201ES60 atm 15/30:0.33:1055, 5282DD83, Limit 110 minutes"
    //val line = "08:03:23 000005F0 Acct-Local:LogOff: hnfdl-150814-636, HN-MP02-8, xe-0/1/0.3180:3180#HNIP50301GC57 PON 0/2/66 70d931467a96 3180 CIGGf2832866, 32799C9E"
//    val line = "11:25:46 0000051C Auth-Local:SignIn: Hnfdl-160524-065, HN-MP01-1, xe-0/0/1.3001:3001#GPON PON 0/1 3001, F84693F7"
    val line = "06:59:59 00000758 Auth-Local:SignIn: Lcdsl-140731-355, LCI-MP-01-01, xe-0/1/0.3001:3001#, 4EE815AF"
    val log = ConLog(line, 1512086400L)
    val line1 = log.toString()
    println(line1)
    val log2 = ConLog(line1)
    val line2 = log2.toString()
    println(line2)
    
    
//    "08:03:22 00001474 Acct-Local:LogOff: tnfdl-151217-842, TNN-MP02, xe-1/1/0.3206:3206#TNNP03101GC56 PON 0/5/11  3206, 904FE611" match {
//      case ConLog.REGEX01(_*) => println("TRUE")
//      case _ => println("FALSE")
//    }
  }
  
  
}