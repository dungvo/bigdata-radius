package com.ftel.bigdata.radius.stats

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.radius.classify.LoadLog

@RunWith(classOf[JUnitRunner])
class LoadStatsSuite extends FunSuite {
  test("TEST PARSE LOAD Stats") {
    val lines = Array(
      """ "ACTALIVE","Dec 01 2017 06:59:59","HCM-MP06-1","192765439","sgdsl-120227-702","89762316","3711159218","3194215772","0","323988","183.80.167.92","4c:f2:bf:69:ea:72","2405:4800:5a84:df95:0000:0000:0000:0000/64","0","3","1246050250","0","3191306576","1" """,
      """ "ACTLOGOF","Dec 01 2017 06:59:59","DNG-MP01-1","-247656230","Dafdl-160413-044","78097412","135119525","2334631273","10","48293","100.99.86.7","70:d9:31:c4:05:de","2405:4800:309f:1db5:0000:0000:0000:0000/64","0","0","119470252","0","2195580203","0" """ ,
      """ "ACTALIVE","Dec 01 2017 06:59:59","HCM-MP06-1","-930271376","sgdsl-061222-503","89762295","34101673","2930192408","0","323994","1.54.136.213","4c:f2:bf:43:2b:0a","2405:4800:5a84:df92:0000:0000:0000:0000/64","10","21","1714387855","0","1813300587","5" """
      )
    lines.foreach(x => {
      val log = LoadStats(LoadLog(x, "2018-01-31").asInstanceOf[LoadLog])
      assert(log.isInstanceOf[LoadStats])
    })
  }

  test("TEST Calculate Donwload/Upload") {
    val line = """ "ACTALIVE","Dec 01 2017 06:59:59","LDG-MP01-2","796176075","Lddsl-161001-360","1905765","477268962","3712614232","0","1011598","100.91.231.187","64:d9:54:82:37:e4","","1","35","0","0","0","0" """
   
    val log = LoadLog(line, "2018-02-12")
    assert(log.isInstanceOf[LoadLog])
    val stats = LoadStats(log.asInstanceOf[LoadLog])
    
    
    assert(stats.timestamp == 1512086399000L)
    assert(stats.name == "Lddsl-161001-360")
    assert(stats.sessionId == "1905765")
    assert(stats.sessionTime == 1011598)
    assert(stats.download == 154036469557L)
    assert(stats.upload == 4772236257L)
    
    
  }
  
  
}
