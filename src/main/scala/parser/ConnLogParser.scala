package parser

import scala.util.matching.Regex
import util.AtomicPattern
/**
  * Created by hungdv on 08/03/2017.
  */
class ConnLogParser extends AbtractLogParser {
  private val c_time2               =   AtomicPattern.time2
  private val c_ssThreadId          =   AtomicPattern.ssThreadId
  private val c_contask             =   AtomicPattern.conTask
  private val c_content             =   AtomicPattern.content
  private val c_nASName             =   AtomicPattern.nASName
  private val c_undefinedText       =   AtomicPattern.unindentified
  private val c_rejectCause         =   AtomicPattern.rejectCause
  private val c_rejectResultDetail  =   AtomicPattern.rejectResultDetail

  private val conRegexPattern: Regex          = AtomicPattern.conRegexPattern
  private val signInLogOffPattern: Regex      = AtomicPattern.signInLogOffRegexPattern
  private val rejectPattern: Regex            = AtomicPattern.rejectRegexPattern
  private val brasLookUp = Predef.Map("HCM-MP03-2-NEW" -> "HCM-MP-03-02",
    "VPC-MP01" -> "VPC-MP-01-01", "HN-MP02-8" -> "HNI-MP-02-08", "LGI-MP01-2" -> "LGI-MP-01-02", "DTP-MP01-2" -> "DTP-MP-01-02",
    "TQG-MP01-1" -> "TQG-MP-01-01", "HDG-MP01-3" -> "HDG-MP-01-03", "VLG-MP01-1" -> "VLG-MP-01-01", "AGG-MP01-1" -> "AGG-MP-01-01",
    "HN-MP01-1" -> "HNI-MP-01-01", "HN-MP05-1" -> "HNI-MP-05-01", "DNI-MP01-2-NEW" -> "DNI-MP-01-02", "VTU-MP01-1-NEW" -> "VTU-MP-01-01",
    "DLK-MP01-2" -> "DLK-MP-01-02", "BDH-MP01-2" -> "BDH-MP-01-02", "QBH-MP01-2" -> "QBH-MP-01-02", "TNH-MP01-1" -> "TNH-MP-01-01",
    "HN-MP03-1" -> "HNI-MP-03-01", "CMU-MP01-2" -> "CMU-MP-01-02", "BNH-MP01-3" -> "BNH-MP-01-03", "THA-MP01-2" -> "THA-MP-01-02",
    "NAN-MP01-3" -> "NAN-MP-01-03", "PYN-MP01-2" -> "PYN-MP-01-02", "HN-MP02-5" -> "HNI-MP-02-05", "TNN-MP03" -> "TNN-MP-01-03",
    "TGG-MP01-2" -> "TGG-MP-01-02", "IXIA-test" -> "QNH-MP-01-02", "KGG-MP01-3" -> "KGG-MP01-3", "LDG-MP01-2" -> "LDG-MP-01-02",
    "QNI-MP01-1" -> "QNI-MP-01-01", "QNH-MP03" -> "QNH-MP-01-03", "YBI-MP02" -> "YBI-MP-01-02", "DLK-MP01-4" -> "DLK-MP-01-04",
    "QNM-MP01-2" -> "QNM-MP-01-02", "BPC-MP01-1" -> "BPC-MP-01-01", "GLI-MP01-2" -> "GLI-MP-01-02", "VTU-MP01-2-NEW" -> "VTU-MP-01-02",
    "LSN-MP02" -> "LSN-MP-01-02", "HCM-MP06-1" -> "HCM-MP-06-01", "HN-MP02-1-NEW" -> "HNI-MP-02-01", "QTI-MP01-1" -> "QTI-MP-01-01",
    "Hostname_Radius" -> "Hostname_Noc", "HN-MP02-2" -> "HNI-MP-02-02", "HCM-MP02-1" -> "HCM-MP-02-01", "BDH-MP01-4" -> "BDH-MP-01-04",
    "HYN-MP01-3" -> "HYN-MP-01-03", "BDG-MP01-1-New" -> "BDG-MP-01-01", "YBI-BRAS01" -> "YBI-MP-01-01", "HN-MP02-7" -> "HNI-MP-02-07",
    "LGI-MP01-1" -> "LGI-MP-01-01", "7200-FCAM-09" -> "TNH-MP-01-01", "HCM-MP05-5" -> "HCM-MP-Backup-01", "DAH-MP02" -> "DAH-MP-01-02",
    "DTP-MP01-1" -> "DTP-MP-01-01", "STY-MP02" -> "STY-MP-01-02", "HP-MP01-NEW" -> "HPG-MP-01-01", "LDG-MP01-4" -> "LDG-MP-01-04",
    "DLK-MP01-1" -> "DLK-MP-01-01", "BDH-MP01-1" -> "BDH-MP-01-01", "HTH-MP02" -> "HTH-MP-01-02", "QBH-MP01-1" -> "QBH-MP-01-01",
    "CTO-MP01-2-NEW" -> "CTO-MP01-2-NEW", "KTM-MP01-2" -> "KTM-MP-01-02", "HYN-MP02" -> "HYN-MP-01-02", "CMU-MP01-1" -> "CMU-MP-01-01",
    "BNH-MP01-2" -> "BNH-MP-01-02", "HCM-QT-MP02-2" -> "HCM-MP-02-02", "BRA-MP01-2" -> "BRA-MP-01-02", "DNG-MP01-1" -> "DNG-MP-01-01",
    "HCM-MP04-1-NEW" -> "HCM-MP-04-01", "TVH-MP01-2" -> "TVH-MP-01-02", "HN-MP02-4" -> "HNI-MP-Backup-01", "BDG-MP01-2-New" -> "BDG-MP-01-02",
    "PTO-MP02" -> "PTO-MP-01-02", "HCM-MP01-2" -> "HCM-MP-01-02", "HCM-MP05-2" -> "HCM-MP-05-02", "NTN-MP01-2" -> "NTN-MP-01-02", "DNG-MP-2" -> "DNG-MP-01-02",
    "THA-MP01-4" -> "THA-MP-01-04", "PYN-MP01-1" -> "PYN-MP-01-01", "NAN-MP01-2" -> "NAN-MP-01-02", "HDG-MP01-2" -> "HDG-MP-01-02", "QNH-MP02" -> "QNH-MP-01-02",
    "CTO-MP01-1" -> "CTO-MP01-1-NEW", "LDG-MP01-1" -> "LDG-MP-01-01", "TQG-MP01-2" -> "TQG-MP-01-02", "HP-MP02-NEW" -> "HPG-MP-01-02", "BLC-MP01-2" -> "BLC-MP-01-02",
    "TGG-MP01-1" -> "TGG-MP-01-01", "KGG-MP01-2" -> "KGG-MP-01-02", "GLI-MP01-1" -> "GLI-MP-01-01", "THA-MP01-1" -> "THA-MP-01-01", "HD-BRAS01" -> "YBI-MP-01-01",
    "BTE-MP01-2" -> "BTE-MP-01-02", "DLK-MP01-3" -> "DLK-MP-01-03", "BDH-MP01-3" -> "BDH-MP-01-03", "NAN-MP01-4" -> "NAN-MP-01-04", "7200-FCAM-08" -> "TNH-MP-01-02",
    "GLI-MP01-4" -> "GLI-MP-01-04", "HCM-MP05-4" -> "HCM-MP-Backup-02", "NTG-MP01-02" -> "NTG-MP-01-02", "DAH-MP01" -> "DAH-MP-01-01", "BTN-MP01-1-NEW" -> "BTN-MP-01-01",
    "STY-MP01" -> "STY-MP-01-01", "TNN-MP02" -> "TNN-MP-01-02", "HDG-MP01-4" -> "HDG-MP-01-04", "QNH-MP04" -> "QNH-MP-01-04", "KTM-MP01-1" -> "KTM-MP-01-01",
    "HYN-MP01" -> "HYN-MP-01-01", "BRA-MP01-1" -> "BRA-MP-01-01", "KGG-MP01-4" -> "KGG-MP01-4", "QNI-MP01-2" -> "QNI-MP-01-02", "VPC-MP02" -> "VPC-MP-01-02",
    "VLG-MP01-2" -> "VLG-MP-01-02", "AGG-MP01-2" -> "AGG-MP-01-02", "LDG-MP01-3" -> "LDG-MP-01-03", "BNH-MP01-1" -> "BNH-MP-01-01", "HTH-MP01" -> "HTH-MP-01-01",
    "HUE-MP01-1-NEW" -> "HUE-MP-01-01", "QNM-MP01-1" -> "QNM-MP-01-01", "LSN-MP01" -> "LSN-MP-01-01", "TNH-MP01-2" -> "TNH-MP-01-02", "HCM-MP01-1" -> "HCM-MP-01-01",
    "BNH-MP01-4" -> "BNH-MP-01-04", "HN-MP03-2" -> "HNI-MP-03-02", "HN-MP05-2" -> "HNI-MP-05-02", "HCM-MP05-1" -> "HCM-MP-05-01", "QTI-MP01-2" -> "QTI-MP-01-02",
    "HN-MP01-2" -> "HNI-MP-01-02", "IXIA" -> "YBI-MP-01-02", "THA-MP01-3" -> "THA-MP-01-03", "PTO-MP01" -> "PTO-MP-01-01", "NAN-MP01-1" -> "NAN-MP-01-01",
    "TVH-MP01-1" -> "TVH-MP-01-01", "HN-MP02-3" -> "HNI-MP-Backup-02", "HCM-MP03-1-NEW" -> "HCM-MP-03-01", "QNH-MP01" -> "QNH-MP-01-01",
    "BTN-MP01-2-NEW" -> "BTN-MP-01-02", "TNN-MP01-NEW" -> "TNN-MP-01-01", "HN-MP02-6" -> "HNI-MP-02-06", "KGG-MP01-1" -> "KGG-MP-01-01",
    "HDG-MP01-1" -> "HDG-MP-01-01", "BLC-MP01-1" -> "BLC-MP-01-01", "DNI-MP01-1-NEW" -> "DNI-MP-01-01", "TNN-MP04" -> "TNN-MP-01-04",
    "BTE-MP01-1" -> "BTE-MP-01-01", "HUE-MP01-2-NEW" -> "HUE-MP-01-02", "NTG-MP01-01" -> "NTG-MP-01-01", "HCM-MP04-2" -> "HCM-MP-04-02",
    "NTN-MP01-1" -> "NTN-MP-01-01", "BPC-MP01-2" -> "BPC-MP-01-02", "GLI-MP01-3" -> "GLI-MP-01-03", "HYN-MP01-4" -> "HYN-MP-01-04",
    "HCM-MP06-2" -> "HCM-MP-06-02", "7200-FCAM-07" -> "BPC-MP-01-02")
  /**
    * Extract conn log object from log - deprecate.
    * @deprecated
    * @param line
    * @return
    */
  def  extractValues(line: String): Option[AbtractLogLine]={
    line match {
      case signInLogOffPattern(c_time2,c_ssThreadId,c_contask,c_conName,c_nASName,c_undefinedText)
        =>  Option(ConnLogLineObject.create(c_time2,c_ssThreadId,c_contask.substring(11,17),c_conName,c_nASName,c_undefinedText))

      case rejectPattern(c_time2,c_ssThreadId,c_contask,c_conName,c_rejectCause,c_rejectResultDetail)
        =>  Option(ConnLogLineObject.create(c_time2,c_ssThreadId,c_contask.substring(11,17),c_conName,c_rejectCause,c_rejectResultDetail))
      //Fixme!!!! Error here !!!!!
      case _ => None
      //case _ => Some(ErroLogLine(line))
        // Filter di thang nao co thuoc tinh null .
      //case _ => Option(ConnLogLineObject("","","","","",""))
    }
  }

  def  extractValues_newFormat(line: String): Option[AbtractLogLine]={
    line match {
      case signInLogOffPattern(c_time2,c_ssThreadId,c_contask,c_conName,c_nASName,c_undefinedText)
      =>  Option(ConnLogLineObject.create(c_time2,c_ssThreadId,c_contask.substring(11,17),brasLookUp.getOrElse(c_conName,"n/a"),c_nASName,c_undefinedText))

      case rejectPattern(c_time2,c_ssThreadId,c_contask,c_conName,c_rejectCause,c_rejectResultDetail)
      =>  Option(ConnLogLineObject.create(c_time2,c_ssThreadId,c_contask.substring(11,17),brasLookUp.getOrElse(c_conName,"n/a"),c_rejectCause,c_rejectResultDetail))
      //Fixme!!!! Error here !!!!!
      case _ => None
      //case _ => Some(ErroLogLine(line))
      // Filter di thang nao co thuoc tinh null .
      //case _ => Option(ConnLogLineObject("","","","","",""))
    }
  }

}
object subStringTest{
  def main(args: Array[String]): Unit = {
    //val stringTest = "Auth-Local:Reject:"
    val stringTest = "Auth-Local:LogOff:"
    //val stringTest = "Auth-Local:SignIn:"

    val subString = stringTest.substring(11,17)
    println(subString)
  }
}