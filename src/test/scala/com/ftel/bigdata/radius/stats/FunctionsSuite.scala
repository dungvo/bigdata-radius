package com.ftel.bigdata.radius.stats

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.radius.classify.LoadLog
import org.apache.spark.sql.SparkSession
import com.ftel.bigdata.radius.classify.Parser

@RunWith(classOf[JUnitRunner])
class FunctionssSuite extends FunSuite {
  test("TEST calculateLoad") {
    val MAX_INT = 1500
    val THRESHOLD = 1100
    val data = Array(
      """ "ACTALIVE","Dec 01 2017 06:59:59","LDG-MP01-2","796176075","Lddsl-161001-360","1","100","1200","0","1011598","100.91.231.187","64:d9:54:82:37:e4","","0","0","0","0","0","0" """,
      """ "ACTALIVE","Dec 01 2017 08:59:59","HCM-MP01-1","1670917756","sgfdl-151006-056","1","90","900","0","1","118.69.111.14","00:0d:48:0e:33:02","","0","0","0","0","0","0" """,
      """ "ACTALIVE","Dec 01 2017 09:59:59","LDG-MP01-2","-1655374348","Lddsl-161001-360","1","50","500","0","445786","100.91.228.125","64:d9:54:bf:b8:28","","0","0","0","0","0","0" """,
      """ "ACTALIVE","Dec 01 2017 10:59:59","LDG-MP01-2","1309903068","sgfdl-151006-056","1","60","1200","0","445789","100.91.228.118","a0:f3:c1:42:41:f7","","0","0","0","0","0","0" """,
      """ "ACTALIVE","Dec 01 2017 11:59:59","HCM-MP06-1","192765439","sgfdl-151006-056","1","30","120","0","323988","183.80.167.92","4c:f2:bf:69:ea:72","2405:4800:5a84:df95:0000:0000:0000:0000/64","0","0","1246050250","0","3191306576","1" """,
      """ "ACTLOGOF","Dec 01 2017 12:59:59","DNG-MP01-1","-247656230","Lddsl-161001-360","1","0","120","10","48293","100.99.86.7","70:d9:31:c4:05:de","2405:4800:309f:1db5:0000:0000:0000:0000/64","0","0","119470252","0","2195580203","0" """,
      """ "ACTALIVE","Dec 01 2017 13:59:59","HCM-MP06-1","-930271376","Lddsl-161001-360","2","10","100","0","323994","1.54.136.213","4c:f2:bf:43:2b:0a","2405:4800:5a84:df92:0000:0000:0000:0000/64","0","0","1714387855","0","1813300587","5" """,
      """ "ACTALIVE","Dec 01 2017 14:59:59","HCM-MP06-1","732492071","sgfdl-151006-056","2","80","1100","0","539973","100.106.36.93","70:d9:31:6e:d1:de","2405:4800:5a97:0925:0000:0000:0000:0000/64","0","0","411622520","0","3380674348","1" """,
      """ "ACTALIVE","Dec 01 2017 15:59:59","HCM-MP06-1","1846345404","Lddsl-161001-360","2","70","1000","0","712771","42.116.210.125","4c:f2:bf:77:c8:b6","2405:4800:5a84:c533:0000:0000:0000:0000/64","0","0","191347017","1","4051559148","31" """)
    
    val day = "2017-12-01"
    val path = "radius-log-sample.csv"
    val sparkSession = SparkSession.builder().appName("local").master("local[4]").getOrCreate()
    val sc = sparkSession.sparkContext

    val lines = sc.parallelize(data, 1) //sc.textFile(path, 1)
    val logs = lines.map(x => Parser.parse(x, day)).filter(x => x.isInstanceOf[LoadLog])

    val loadStats = logs.map(x => LoadStats(x.asInstanceOf[LoadLog]))
    
    val rows = Functions.calculateLoad(sparkSession, loadStats, MAX_INT, THRESHOLD)
    
    //val rows = df.rdd
    
    val contract1 = rows
      .filter(x => x.name == "Lddsl-161001-360")
      .map(x => x.sessionId -> (x.downloadUsage, x.uploadUsage))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      .collect()
      .toMap
    assert(contract1.get("1").get._1 == 3120)
    assert(contract1.get("1").get._2 == 150)
    
    assert(contract1.get("2").get._1 == 1000)
    assert(contract1.get("2").get._2 == 70)
    
    val contract2 = rows
      .filter(x => x.name == "sgfdl-151006-056")
      .map(x => x.sessionId -> (x.downloadUsage, x.uploadUsage))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      .collect()
      .toMap
    assert(contract2.get("1").get._1 == 1620)
    assert(contract2.get("1").get._2 == 180)
    
    assert(contract2.get("2").get._1 == 1100)
    assert(contract2.get("2").get._2 == 80)
  }
}
