package com.ftel.bigdata.radius.classify

import com.ftel.bigdata.radius.utils.BrasUtil

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
    date: String,
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
  override def get() = "ConLog-" + typeLog
  def this(date: String, session: String, typeLog: String, name: String, text: String) = this(date, session, typeLog, name, null, null, null, null, 0, null, text)
//  def this(date: String, session: String, typeLog: String, name: String, arr: Array[String]) = {
//    
//  }
}
//
//trait ConLogGeneric
//
//class ConLogNormal(
//    nasName: String,
//    card: Card,     // xe-3/1/1.3105:3105#HNIP48702GC57
//    cable: Cable,   // PON 0/1/103
//    mac: String,
//    vlan: Int,
//    serialONU: String,
//    text: String) extends ConLogGeneric
//    
//case class SignInLog(
//    nasName: String,
//    card: Card,     // xe-3/1/1.3105:3105#HNIP48702GC57
//    cable: Cable,   // PON 0/1/103
//    mac: String,
//    vlan: Int,
//    serialONU: String,
//    text: String) extends ConLogNormal(nasName, card, cable, mac, vlan, serialONU, text)
//
//case class LogOffLog(
//    nasName: String,
//    card: Card,     // xe-3/1/1.3105:3105#HNIP48702GC57
//    cable: Cable,   // PON 0/1/103
//    mac: String,
//    vlan: Int,
//    serialONU: String,
//    text: String) extends ConLogNormal(nasName, card, cable, mac, vlan, serialONU, text)
//
//case class RejectLog(text: String) extends ConLogGeneric

/**
 * xe-3/1/1.3105:3105#HNIP48702GC57
 */
case class Card(
    lineId: Int,
    id: Int,
    port: Int,
    vlan: Int,
    olt: String)

/**
 * xe-1/1/1.3619:3619#HNIP51101GC57
 */
object Card {
  def apply(text: String): Card = {
    val regex = s"xe-(\\d)/(\\d)/(\\d).(\\d{4}):\\d{4}#(\\w)".r("lineId", "id", "port", "vlan", "olt")
    text match {
      case regex(lineId, id, port, vlan, olt) => Card(lineId.toInt, id.toInt, port.toInt, vlan.toInt, olt)
      case _ => null
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
  def this(arr: Array[String]) = this(arr(0).toInt, arr(1).toInt, arr(2).toInt)
}

    
object ConLog {
  /**
   * Some format wrong such as:
   *  - Reject
   *  - 07:00:00,00000A64,Acct-Local:LogOff,Ctfdl-160528-397,CTO-MP01-1-NEW,77BACF41
   *  - 08:03:22,00006140,Acct-Local:LogOff,ktdsl-140303-967,GLI-MP01-1,xe-1/3/0.1050:1050#KTMP01201ES52 atm 1/56:0.33:1050, 1000041A
   *  - 08:03:23,000017C4,Acct-Local:LogOff,Dndsl-140814-648,DNI-MP01-1-NEW,xe-5/1/1.1285:1285#DNIP00501GC56 PON 0/6/45  1285, D703731A
   *  - 08:03:23,0000A1F4,Acct-Local:LogOff,Ppdsl-150312-272,MX480-03,EF1D8D59
   * Những log này là log của những thiết bị mà mình chưa quan tâm tới, thiết bị đó nó không chứa đủ thông tin log.
   */
  private val SIZE_LENGTH = 5
  
  private val SIGNIN = "Auth-Local:SignIn:" -> "SignIn"
  private val LOGOFF = "Acct-Local:LogOff:" -> "LogOff"
  private val REJECT = "Auth-Local:Reject:" -> "Reject"
  
  def apply(line: String): AbstractLog = {
    val arr = line.split(" ")
    val size = arr.size
    if (size > SIZE_LENGTH) {
      val date: String = arr(0)
      val session: String = arr(1)
      val typeLog: String = getType(arr(2))
      val name: String = arr(3)
      val text: String = arr.slice(4, size).mkString("\t")
      if (typeLog != null) {
        if (typeLog == REJECT._2) new ConLog(date, session, typeLog, name, text)
        else if (typeLog == SIGNIN._2 || typeLog == LOGOFF._2) {
          getConLog(date, session, typeLog, name, text)
        } else ErrLog(line)
      } else ErrLog(line)
    } else ErrLog(line)
  }

  private def getConLog(date: String, session: String, typeLog: String, name: String, text: String): ConLog = {
    val arr = text.split("\\s+")
    val size = arr.size
    size match {
      // HNI-MP-06-01, xe-1/1/1.3619:3619#HNIP51101GC57 PON 0/1/95 a85840286756 3619 FPTT15c000c1, E4C1819B
      case 8 => {
        val nasName: String = BrasUtil.BRAS_RADIUS_MAPPING.getOrElse(BrasUtil.trim(arr(0), ","), "N/A")
        val card = Card(arr(1).trim())
        val cable = new Cable(arr(3).split("/"))
        val mac = arr(4)
        val vlan = arr(5).toInt
        val serialONU: String = arr(6)
        val textRemain: String = arr(7)
        //println("8 => " + text)
        ConLog(date, session, typeLog, name, nasName, card, cable, mac, vlan, serialONU, textRemain)
      }
      case 6 => {
        val nasName: String = BrasUtil.BRAS_RADIUS_MAPPING.getOrElse(BrasUtil.trim(arr(0), ","), "N/A")
        val card = Card(arr(1).trim())
        val cable = new Cable(arr(3).split("/"))
        val mac = null //arr(4)
        val vlan = BrasUtil.trim(arr(4), ",").toInt
        val serialONU: String = null
        val textRemain: String = null
        //println("5 => " + text)
        ConLog(date, session, typeLog, name, nasName, card, cable, mac, vlan, serialONU, textRemain)
      }
      case 5 => {
        val nasName: String = BrasUtil.BRAS_RADIUS_MAPPING.getOrElse(BrasUtil.trim(arr(0), ","), "N/A")
        val card = Card(arr(1).trim())
        val cable = null //new Cable(arr(3).split("/"))
        val mac = null //arr(4)
        val vlan = if (card != null) card.vlan else -1
        val serialONU: String = null
        val textRemain: String = null
        //println("5 => " + text)
        ConLog(date, session, typeLog, name, nasName, card, cable, mac, vlan, serialONU, textRemain)
      }
      case 3 => {
        val nasName: String = BrasUtil.BRAS_RADIUS_MAPPING.getOrElse(BrasUtil.trim(arr(0), ","), "N/A")
        val card = Card(arr(1).trim())
        val cable = null //new Cable(arr(3).split("/"))
        val mac = null //arr(4)
        val vlan = if (card != null) card.vlan else -1
        val serialONU: String = null
        val textRemain: String = null
        //println("5 => " + text)
        ConLog(date, session, typeLog, name, nasName, card, cable, mac, vlan, serialONU, textRemain)
      }
      case _ => {
        println("TEXT => " + text + s"[Size: $size]")
        new ConLog(date, session, typeLog, name, text)
      }
    }
  }
  
  private def getType(typeLog: String): String = {
    if (typeLog == SIGNIN._1) SIGNIN._2
    else if (typeLog == LOGOFF._1) LOGOFF._2
    else if (typeLog == REJECT._1) REJECT._2
    else null
  }
}