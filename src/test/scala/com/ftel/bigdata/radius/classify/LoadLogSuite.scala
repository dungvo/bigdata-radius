package com.ftel.bigdata.radius.classify

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.ftel.bigdata.utils.DateTimeUtil

@RunWith(classOf[JUnitRunner])
class LoadLogSuite extends FunSuite {
  test("TEST PARSE LOAD LOG") {
    val lines = Array(
      """ "ACTALIVE","Dec 01 2017 06:59:59","HCM-MP06-1","192765439","sgdsl-120227-702","89762316","3711159218","3194215772","0","323988","183.80.167.92","4c:f2:bf:69:ea:72","2405:4800:5a84:df95:0000:0000:0000:0000/64","0","3","1246050250","0","3191306576","1" """,
      """ "ACTLOGOF","Dec 01 2017 06:59:59","DNG-MP01-1","-247656230","Dafdl-160413-044","78097412","135119525","2334631273","10","48293","100.99.86.7","70:d9:31:c4:05:de","2405:4800:309f:1db5:0000:0000:0000:0000/64","0","0","119470252","0","2195580203","0" """ ,
      """ "ACTALIVE","Dec 01 2017 06:59:59","HCM-MP06-1","-930271376","sgdsl-061222-503","89762295","34101673","2930192408","0","323994","1.54.136.213","4c:f2:bf:43:2b:0a","2405:4800:5a84:df92:0000:0000:0000:0000/64","10","21","1714387855","0","1813300587","5" """,
      """ "ACTALIVE","Oct 01 2017 06:59:59","KTM-MP01-2","-1639172390","ktdsl-140905-396","3116602","528510406","-904169345","0","739775","100.94.65.35","bc:96:80:36:1a:24","" """,
      """ "Dec 01 2017 19:57:53","PFSENSE25543","0","tndsl-160405-836","c3e0c13593db619a","1016945","16940655","0","2823","192.168.177.120","" """
      )
    lines.foreach(x => {
      val log = LoadLog(x, DateTimeUtil.create("2018-01-31", DateTimeUtil.YMD).getMillis)
      assert(log.isInstanceOf[LoadLog])
    })
  }

  test("TEST PARSE LOAD LOG Full Format") {
    val line = """ "ACTALIVE","Dec 01 2017 06:59:59","HCM-MP06-1","192765439","sgdsl-120227-702","89762316","3711159218","3194215772","0","323988","183.80.167.92","4c:f2:bf:69:ea:72","2405:4800:5a84:df95:0000:0000:0000:0000/64","0","3","1246050250","0","3191306576","1" """
   
    val log = LoadLog(line, DateTimeUtil.create("2018-12-01", DateTimeUtil.YMD).getMillis)
    assert(log.isInstanceOf[LoadLog])
    val loadLog = log.asInstanceOf[LoadLog]
    assert(loadLog.statusType == "ACTALIVE")
    assert(DateTimeUtil.create(loadLog.timestamp / 1000).toString(LoadLog.DATE_TIME_PATTERN) == "Dec 01 2017 06:59:59")
    assert(loadLog.nasName == "HCM-MP06-1")
    assert(loadLog.nasPort == 192765439)
    assert(loadLog.name == "sgdsl-120227-702")
    
    assert(loadLog.sessionID == "89762316")
    assert(loadLog.input == 3711159218L)
    assert(loadLog.output == 3194215772L)

    assert(loadLog.termCode == 0)
    assert(loadLog.sessionTime == 323988)
    assert(loadLog.ipAddress == "183.80.167.92")
    assert(loadLog.callerID == "4c:f2:bf:69:ea:72")
    
    assert(loadLog.ipv6Address == "2405:4800:5a84:df95:0000:0000:0000:0000/64")
    assert(loadLog.inputG == 0)
    assert(loadLog.outputG == 3)
    
    assert(loadLog.inputIPv6 == 1246050250L)
    assert(loadLog.inputIPv6G == 0)
    assert(loadLog.outputIPv6 == 3191306576L)
    assert(loadLog.outputIPv6G == 1)
  }
  
  test("TEST PARSE LOAD LOG Patial Format 1") {
    val line = """ "Dec 01 2017 19:57:53","PFSENSE25543","0","tndsl-160405-836","c3e0c13593db619a","1016945","16940655","0","2823","192.168.177.120","" """
   
    val log = LoadLog(line, DateTimeUtil.create("2018-01-31", DateTimeUtil.YMD).getMillis)
    assert(log.isInstanceOf[LoadLog])
    val loadLog = log.asInstanceOf[LoadLog]
    assert(loadLog.statusType == "UNKNOWN")
    assert(DateTimeUtil.create(loadLog.timestamp / 1000).toString(LoadLog.DATE_TIME_PATTERN) == "Dec 01 2017 19:57:53")
    assert(loadLog.nasName == "PFSENSE25543")
    assert(loadLog.nasPort == 0)
    assert(loadLog.name == "tndsl-160405-836")
    
    assert(loadLog.sessionID == "c3e0c13593db619a")
    assert(loadLog.input == 1016945L)
    assert(loadLog.output == 16940655L)

    assert(loadLog.termCode == 0)
    assert(loadLog.sessionTime == 2823)
    assert(loadLog.ipAddress == "192.168.177.120")
    assert(loadLog.callerID == "")
    
    assert(loadLog.ipv6Address == null)
    assert(loadLog.inputG == 0)
    assert(loadLog.outputG == 0)
    
    assert(loadLog.inputIPv6 == 0)
    assert(loadLog.inputIPv6G == 0)
    assert(loadLog.outputIPv6 == 0)
    assert(loadLog.outputIPv6G == 0)
  }
  
  test("TEST PARSE LOAD LOG Patial Format 2") {
    val line = """ "ACTALIVE","Oct 01 2017 06:59:59","KTM-MP01-2","-1639172390","ktdsl-140905-396","3116602","528510406","-904169345","0","739775","100.94.65.35","bc:96:80:36:1a:24","" """
   
    val log = LoadLog(line, DateTimeUtil.create("2017-10-01", DateTimeUtil.YMD).getMillis)
    assert(log.isInstanceOf[LoadLog])
    val loadLog = log.asInstanceOf[LoadLog]
    assert(loadLog.statusType == "ACTALIVE")
    assert(DateTimeUtil.create(loadLog.timestamp / 1000).toString(LoadLog.DATE_TIME_PATTERN) == "Oct 01 2017 06:59:59")
    assert(loadLog.nasName == "KTM-MP01-2")
    assert(loadLog.nasPort == -1639172390)
    assert(loadLog.name == "ktdsl-140905-396")
    
    assert(loadLog.sessionID == "3116602")
    assert(loadLog.input == 528510406L)
    assert(loadLog.output == -904169345L)

    assert(loadLog.termCode == 0)
    assert(loadLog.sessionTime == 739775)
    assert(loadLog.ipAddress == "100.94.65.35")
    assert(loadLog.callerID == "bc:96:80:36:1a:24")
    
    assert(loadLog.ipv6Address == null)
    assert(loadLog.inputG == 0)
    assert(loadLog.outputG == 0)
    
    assert(loadLog.inputIPv6 == 0)
    assert(loadLog.inputIPv6G == 0)
    assert(loadLog.outputIPv6 == 0)
    assert(loadLog.outputIPv6G == 0)
  }
}
