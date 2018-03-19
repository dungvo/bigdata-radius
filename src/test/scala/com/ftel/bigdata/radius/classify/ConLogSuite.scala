package com.ftel.bigdata.radius.classify

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.ftel.bigdata.utils.DateTimeUtil

@RunWith(classOf[JUnitRunner])
class ConLogSuite extends FunSuite {
  test("TEST PARSE CON LOG") {
    val lines = Array(
      "06:59:59 0000023C Auth-Local:SignIn: hnfdl-120213-213, HN-MP01-1, xe-4/0/1.3020:3020#HNIP00801GC57 PON 0/5/21 001daa61ad41 3020 CIGGe2782074, 4AE6E8C3",
      "06:59:59 00000A08 Auth-Local:Reject: hndsl-070723-438, Result 103, No DSP on port.fpt.net (fc:b6:98:33:57:5e)",
      "06:59:59 00001074 Auth-Local:Reject: Hpdsl-111005-127, Result 103, No DSP on port.fpt.net (70:d9:31:bb:63:4e)",
      "06:59:59 0000097C Auth-Local:Reject: hnfdl-170119-306, Result 5, Out Of Office (4c:fa:ca:cd:11:2e)",
      "06:59:59 00001178 Auth-Local:Reject: Bnfdl-161016-305, Result 103, No DSP on port.fpt.net (78:d9:9f:cf:fa:3c)",
      "06:59:59 00000E30 Auth-Local:Reject: Thdsl-170119-929, Result 5, Out Of Office (64:d9:54:bf:57:88)",
      "06:59:59 00000550 Auth-Local:Reject: hnfdl-150203-669, Result 103, No DSP on port.fpt.net (fc:b6:98:20:46:52)",
      "06:59:59 000016E8 Auth-Local:Reject: Hnfdl-170210-450, Result 5, Out Of Office (9c:50:ee:5e:2b:be)",
      "06:59:59 00001524 Auth-Local:Reject: Hnfdl-160603-375, Result 5, Out Of Office (a8:58:40:53:6e:5e)",
      "06:59:59 000014B8 Auth-Local:Reject: Hpfdl-160630-081, Result 103, No DSP on port.fpt.net (a8:58:40:4a:8a:7e)",
      "06:59:59 000014EC Auth-Local:Reject: hndsl-121027-604, Result 103, No DSP on port.fpt.net (64:70:02:47:d9:73)",
      "06:59:59 00001208 Auth-Local:Reject: Hnfdl-170213-042, Result 5, Out Of Office (9c:50:ee:5d:74:36)",
      "06:59:59 00000A04 Auth-Local:Reject: Hnfdl-170203-852, Result 103, No DSP on port.fpt.net (9c:50:ee:2c:2c:56)",
      "06:59:59 00001318 Auth-Local:Reject: hnfdl-160519-118, Result 5, Out Of Office (a8:58:40:77:fa:fe)",
      "06:59:59 00000F40 Auth-Local:Reject: Hnfdl-160325-036, Result 103, No DSP on port.fpt.net (a8:58:40:3a:ea:86)",
      "06:59:59 000000C4 Auth-Local:Reject: Hpdsl-151003-429, Result 103, No DSP on port.fpt.net (bc:96:80:17:ff:34)",
      "06:59:59 0000070C Auth-Local:SignIn: Thdsl-130731-568, THA-MP01-2, xe-0/3/0.3903:3903#THAP06301GC57 PON 0/5/19 9c50eedefc8e 3903 FPTT1750fd6d, 8CB116DB",
      "06:59:59 00001218 Auth-Local:Reject: Hndsl-141029-267, Result 5, Out Of Office (4c:f2:bf:76:90:66)",
      "06:59:59 000010E8 Auth-Local:SignIn: Htdsl-160905-966, HTH-MP02, xe-1/3/0.3005:3005#HTHP00201ES50 atm 8/24:0.33:3005, 820364B5",
      "06:59:59 00000714 Auth-Local:Reject: Hpdsl-170123-283, Result 103, No DSP on port.fpt.net (a8:c8:3a:c8:75:7a)",
      "06:59:59 0000095C Auth-Local:Reject: Hpfdl-160726-951, Result 103, No DSP on port.fpt.net (a8:58:40:4a:68:8e)",
      "06:59:59 00000B08 Auth-Local:Reject: Hnfdl-160708-745, Result 5, Out Of Office (fc:b6:98:a8:c4:26)",
      "06:59:59 00000758 Auth-Local:SignIn: Lcdsl-140731-355, LCI-MP-01-01, xe-0/1/0.3001:3001#, 4EE815AF",
      "06:59:59 000007CC Auth-Local:Reject: hnfdl-150821-124, Result 103, No DSP on port.fpt.net (fc:b6:98:61:6d:46)",
      "06:59:59 00001304 Auth-Local:Reject: hnfdl-160314-954, Result 103, No DSP on port.fpt.net (ec:08:6b:73:b8:09)",
      "06:59:59 00001398 Auth-Local:Reject: Hndsl-090831-091, Result 7, Access Denied From CallerID (4c:f2:bf:1f:2c:06)",
      "06:59:59 00001350 Auth-Local:Reject: Slfdl-150608-589, Result 103, No DSP on port.fpt.net (fc:b6:98:17:ae:86)",
      "06:59:59 00001308 Auth-Local:Reject: bndsl-140507-112, Result 103, No DSP on port.fpt.net (bc:96:80:db:7f:cc)",
      "06:59:59 000010FC Auth-Local:SignIn: Hnfdl-160306-702, HN-MP05-1, xe-8/1/0.3407:3407#HNIP12202GC57 PON 0/7/42 70d931cba436 3407 CIGGf4074549, B6F8694",
      "06:59:59 00001144 Auth-Local:Reject: Hnfdl-160617-063, Result 5, Out Of Office (a8:58:40:11:81:6e)",
      "06:59:59 000017D4 Auth-Local:Reject: hndsl-140704-021, Result 40, Unauthorize xDSL access from Mac Address (4c:f2:bf:b5:ee:c6)",
      "06:59:59 00000C38 Auth-Local:SignIn: hndsl-140628-279, HN-MP03-1, xe-1/3/0.3101:3101#HNIP00401GC57 PON 0/6/61 fcb69832a79e 3101 CIGGf0113076, 8A8C7D5E",
      "06:59:59 00000A5C Auth-Local:SignIn: hnfdl-160109-218, HN-MP01-2, xe-4/0/1.3146:3146#HNIP26301GC57 PON 0/3/80 a858400246d6 3146 CIGGf4609674, 634881B8",
      "08:03:22 000013BC Acct-Local:LogOff: Hndsl-140724-655, HN-MP02-5, xe-3/1/1.3303:3303#HNIP42102GC57 PON 0/5/89 a4c7defaf24e 3303 CIGGe2411571, 1F4CC8CB",
      "08:03:22 00000928 Acct-Local:LogOff: qadsl-141022-542, QNM-MP01-1, xe-1/1/0.1012:1012#QNMP01201ES52 atm 10/16:0.33:1012, 100003F4",
      "08:03:22 000017F0 Acct-Local:LogOff: dnfdl-150922-120, DNI-MP01-2-NEW, xe-3/1/1.1346:1346#DNIP02607GC16 PON 0/7/23 a8584051736e 1346 FPTT15c20aab, 10D1203B",
      "08:03:22 00000B54 Acct-Local:LogOff: sgdsl-130116-969, HCM-QT-MP02-2, xe-8/3/0.1256:1256#HCMP47502GC57 PON 0/4/24 fcb69829e20e 1256 CIGGe5251676, 57BDC481",
      "08:03:22 00000FC4 Acct-Local:LogOff: dnfdl-170302-428, DNI-MP01-2-NEW, xe-4/3/1.1346:1346#DNIP02609GC16 PON 0/3/54 fcb6981e8fc2 1346 CIGGe5082804, DD3219EE",
      "08:03:23 000005F0 Acct-Local:LogOff: hnfdl-150814-636, HN-MP02-8, xe-0/1/0.3180:3180#HNIP50301GC57 PON 0/2/66 70d931467a96 3180 CIGGf2832866, 32799C9E",
      "08:03:22 00001474 Acct-Local:LogOff: tnfdl-151217-842, TNN-MP02, xe-1/1/0.3206:3206#TNNP03101GC56 PON 0/5/11  3206, 904FE611",
      "08:03:23 0000A1F4 Acct-Local:LogOff: Ppdsl-150312-272, MX480-03, EF1D8D59",
      "08:03:22 00001538 Acct-Local:LogOff: hpfdl-150912-816, HP-MP02-NEW, xe-0/1/1.3015:3015#HPGP06201GC56 PON 0/6/45  3015, 6F2664FD",
      "08:03:23 0000045C Acct-Local:LogOff: hnfdl-160906-528, HNI-MP-06-01, xe-1/1/1.3619:3619#HNIP51101GC57 PON 0/1/95 a85840286756 3619 FPTT15c000c1, E4C1819B",
      "08:03:23 00000B54 Acct-Local:LogOff: hnfdl-170617-854, HN-MP02-7, xe-8/2/1.3180:3180#HNIP50301GC57 PON 0/2/34 9c50eeb71cf6 3180 FPTT1740404e, A2FD848D",
      "08:03:23 00000C0C Acct-Local:LogOff: hnfdl-150915-161, HN-MP02-8, xe-0/2/0.3180:3180#HNIP50301GC57 PON 0/1/49 70d931565a96 3180 CIGGf2904779, 8413CF60",
      "08:03:23 00000EC0 Acct-Local:LogOff: Hnfdl-160612-088, HN-MP02-7, xe-4/1/0.3180:3180#HNIP50301GC57 PON 0/1/27 a85840f1572e 3180 FPTT1620796a, 5AB6D52D",
      "08:03:22 00006140 Acct-Local:LogOff: ktdsl-140303-967, GLI-MP01-1, xe-1/3/0.1050:1050#KTMP01201ES52 atm 1/56:0.33:1050, 1000041A",
      "08:03:23 000017C4 Acct-Local:LogOff: Dndsl-140814-648, DNI-MP01-1-NEW, xe-5/1/1.1285:1285#DNIP00501GC56 PON 0/6/45  1285, D703731A",
      "08:03:23 00003D98 Acct-Local:LogOff: Sgfdl-160824-122, HCM-QT-MP02-2, xe-7/3/0.1213:1213#HCMP52302GC57 PON 0/4/54 a85840f0e5be 1213 FPTT16206b3c, D460848D",
      "08:03:23 00005C64 Acct-Local:LogOff: Dnfdl-151128-526, DNI-MP01-2-NEW, xe-4/1/1.1346:1346#DNIP02609GC16 PON 0/12/107 70d931e7e02e 1346 CIGGf4309794, 2D76499B",
      "08:03:24 00001318 Acct-Local:LogOff: Dndsl-140814-648, DNI-MP01-1-NEW, xe-5/1/1.1285:1285#DNIP00501GC56 PON 0/6/45  1285, D703731A",
      "08:03:23 000011B0 Acct-Local:LogOff: Hndsl-130425-758, HN-MP02-7, xe-4/0/0.3108:3108#HNIP46704GC57 PON 0/5/29 4cf2bf21a86e 3108 CIGGe2922193, 13490E9C",
      "08:03:23 00000B58 Acct-Local:LogOff: hndsl-140714-295, HN-MP02-8, xe-0/3/0.3180:3180#HNIP50301GC57 PON 0/2/48 4cf2bfbc3016 3180 CIGGe4435109, C8612709",
      "08:03:23 00000824 Acct-Local:LogOff: Htdsl-161222-320, HTH-MP02, xe-1/3/0.3006:3006#HTHP00402ES52 atm 8/57:0.33:3006, BC929DA5",
      "09:58:52 00001374 Auth-Local:SignIn: btfdl-111014-089, BTN-MP01-2-NEW, xe-2/3/0.1602:1602#BTNP00101ES60 eth 17/10:1602, 48888F97",
      "08:59:14 0000175C Acct-Local:LogOff: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-5/1/1.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49",
      "08:59:27 00000A8C Acct-Local:LogOff: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-4/3/0.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49",
      "09:05:50 00000834 Acct-Local:LogOff: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-4/3/0.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49",
      "09:05:52 00001264 Auth-Local:SignIn: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-4/3/0.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49",
      "09:06:03 00000BD8 Auth-Local:SignIn: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-4/1/0.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49",
      "09:07:53 0000671C Acct-Local:LogOff: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-4/3/0.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49",
      "09:11:49 00001088 Acct-Local:LogOff: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-4/1/0.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49",
      "09:39:24 000013B4 Auth-Local:SignIn: Bedsl-160927-319, BTE-MP01-1, xe-0/1/0.1055:1055#BTEP00201ES60 atm 15/30:0.33:1055, 5282DD83, Limit 110 minutes",
      "11:23:20 00000658 Acct-Local:LogOff: Hnfdl-160524-065, HN-MP01-1, xe-0/1/1.3001:3001#GPON PON 0/1 3001, F84693F7",
      "11:25:46 0000051C Auth-Local:SignIn: Hnfdl-160524-065, HN-MP01-1, xe-0/0/1.3001:3001#GPON PON 0/1 3001, F84693F7",
      "09:11:49 00001088 Acct-Local:LogOff: dnfdl-090613- 563, DNI-MP01-1-NEW, xe-4/1/0.1450:1450#DNIP02501ES60 eth 17/16:1450, AA828A49",
      "09:39:24 000013B4 Auth-Local:SignIn: Bedsl-160927-319, BTE-MP01-1, xe-0/1/0.1055:1055#BTEP00201ES60 atm 15/30:0.33:1055, 5282DD83, Limit 110 minutes",
      "11:23:20 00000658 Acct-Local:LogOff: Hnfdl-160524-065, HN-MP01-1, xe-0/1/1.3001:3001#GPON PON 0/1 3001, F84693F7",
      "11:25:46 0000051C Auth-Local:SignIn: Hnfdl-160524-065, HN-MP01-1, xe-0/0/1.3001:3001#GPON PON 0/1 3001, F84693F7"
      )
    lines.foreach(x => {
      val log = ConLog(x, DateTimeUtil.create("2018-01-31", DateTimeUtil.YMD).getMillis )
      assert(log.isInstanceOf[ConLog])
      assert(!log.asInstanceOf[ConLog].nasName.contains("xe"))
      println("BRAS: " + log.asInstanceOf[ConLog].nasName)
      println("BRAS: " + log.asInstanceOf[ConLog].toString())
    })
  }

  test("TEST PARSE CON LOG With Text size is 8 ") {
    val line = "08:03:23 000005F0 Acct-Local:LogOff: hnfdl-150814-636, HN-MP02-8, xe-0/1/0.3180:3180#HNIP50301GC57 PON 0/2/66 70d931467a96 3180 CIGGf2832866, 32799C9E"
    val log = ConLog(line, DateTimeUtil.create("2018-01-31", DateTimeUtil.YMD).getMillis)
    assert(log.isInstanceOf[ConLog])
    val conLog = log.asInstanceOf[ConLog]
    assert(conLog.session == "000005F0")
    assert(conLog.typeLog == "LogOff")
    assert(conLog.name == "hnfdl-150814-636")
    assert(conLog.nasName == "hni-mp-02-08")
    assert(conLog.serialONU == "CIGGf2832866")

    assert(conLog.vlan == 3180)
    assert(conLog.mac == "70d931467a96")

    assert(conLog.card.lineId == 0)
    assert(conLog.card.id == 1)
    assert(conLog.card.port == 0)
    assert(conLog.card.olt == "HNIP50301GC57")

    assert(conLog.cable.number == 0)
    assert(conLog.cable.ontId == 2)
    assert(conLog.cable.indexId == 66)
  }

  test("TEST PARSE LOAD LOG") {
    val line = "08:03:23 000005F0 Acct-Local:LogOff: hnfdl-150814-636, HN-MP02-8, xe-0/1/0.3180:3180#HNIP50301GC57 PON 0/2/66 70d931467a96 3180 CIGGf2832866, 32799C9E"
    val log = ConLog(line, DateTimeUtil.create("2018-01-31", DateTimeUtil.YMD).getMillis)
    assert(log.isInstanceOf[ConLog])
    val conLog = log.asInstanceOf[ConLog]
    assert(conLog.session == "000005F0")
    assert(conLog.typeLog == "LogOff")
    assert(conLog.name == "hnfdl-150814-636")
    assert(conLog.nasName == "hni-mp-02-08")
    assert(conLog.serialONU == "CIGGf2832866")

    assert(conLog.vlan == 3180)
    assert(conLog.mac == "70d931467a96")

    assert(conLog.card.lineId == 0)
    assert(conLog.card.id == 1)
    assert(conLog.card.port == 0)
    assert(conLog.card.olt == "HNIP50301GC57")

    assert(conLog.cable.number == 0)
    assert(conLog.cable.ontId == 2)
    assert(conLog.cable.indexId == 66)
  }
}
