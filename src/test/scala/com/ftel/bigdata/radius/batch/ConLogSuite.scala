package com.ftel.bigdata.radius.batch

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.ftel.bigdata.utils.DateTimeUtil
import com.ftel.bigdata.utils.StringUtil
import org.apache.spark.sql.SparkSession
import com.ftel.bigdata.radius.classify.ConLog

@RunWith(classOf[JUnitRunner])
class ConLogDriverSuite extends FunSuite {
  test("Test Group Brash by minutes") {
    val datesampe = """
1520537759000	00000E14	LogOff	bndsl-150312-802	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	51	fcb69817244e	3103	CIGGe4936588	4239313C
1520537759000	000016E4	LogOff	Bnfdl-170702-751	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	26	9c50ee955186	3103	FPTT17200961	8D926B07
1520537759000	0000141C	LogOff	Bnfdl-171108-088	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	49	5c1a6f1cb626	3103	FPTT1780bf4d	A1BA664F
1520537762000	00000CB0	LogOff	Bnfdl-171209-892	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	53	5c1a6f5749d6	3103	FPTT17a04683	F1BB75FE
1520537763000	000006C0	LogOff	Bnfdl-171015-351	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	45	5c1a6f079f16	3103	FPTT17708d11	156909CE
1520537763000	00000804	LogOff	Bnfdl-170522-927	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	15	9c50ee8d4366	3103	FPTT17100b4b	CD70452F
1520537768000	00000B80	LogOff	Bnfdl-180207-560	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	65	5c1a6f94feae	3103	FPTT17c0acb2	DBE1A296
1520537769000	00001550	LogOff	Bnfdl-151124-562	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	37	fcb69847133e	3103	CIGGf0684442	9FEFEBF6
1520537769000	00001250	LogOff	Bnfdl-141215-299	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	25	4cf2bf1ff986	3103	CIGGe2924864	1DC948F3
1520537775000	00001668	LogOff	Bnfdl-160507-095	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	125	a85840532c7e	3103	FPTT15c241cd	9DB88081
1520537775000	000010B0	LogOff	bndsl-120910-992	bnh-mp-01-02	0	3	0		-1	-1	-1	null	3102	null	null
1520537775000	00000B50	LogOff	Bnfdl-171005-189	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	43	5c1a6f0799de	3103	FPTT17708c6a	B3A9BF65
1520537775000	00000E54	LogOff	Bnfdl-171220-361	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	56	5c1a6f5744de	3103	FPTT17a045e4	F1B3AD6D
1520537775000	000006DC	LogOff	Bnfdl-170830-252	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	34	9c50eee7dc6e	3103	FPTT17605492	10129035
1520537775000	00000D64	LogOff	Bnfdl-170914-651	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	40	9c50eee7d45e	3103	FPTT17605390	6DE5F4ED
1520537775000	0000154C	LogOff	Bnfdl-180114-820	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	60	5c1a6f94815e	3103	FPTT17c09d08	C67AD399
1520537775000	000015A8	LogOff	Bnfdl-180124-941	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	63	5c1a6f9482d6	3103	FPTT17c09d37	850D102C
1520537776000	000011F8	LogOff	Bnfdl-180105-591	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	58	5c1a6f1cbafe	3103	FPTT1780bfe8	DBD5C94A
1520537778000	000017E4	LogOff	Bndsl-150402-501	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	47	fcb69817570e	3103	CIGGe4971239	86C087C6
1520537779000	000015F8	LogOff	Bnfdl-170120-126	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	5	9c50ee28e0e6	3103	FPTT16b05a89	3E959D26
1520537780000	0000148C	LogOff	Bnfdl-170822-140	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	33	9c50eee7d1f6	3103	FPTT17605343	39CF0A06
1520537782000	00001164	SignIn	Bnfdl-160302-761	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	120	a85840377b7e	3103	FPTT15c087e6	DFC44500
1520537782000	00000D0C	LogOff	bnfdl-151218-285	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	106	70d931c626f6	3103	CIGGf4006019	CE839FB1
1520537812000	000009F8	SignIn	bnfdl-151021-797	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	92	70d931b60f5e	3103	CIGGf3616311	D010CD34
1520537818000	00001354	SignIn	Bnfdl-151017-786	bnh-mp-01-02	0	3	0	BNHP02701GC57	0	1	7	70d931b59756	3103	CIGGf3612947	6D3BDD51
1520538323000	00000738	SignIn	bndsl-130620-416	bnh-mp-01-02	-1	-1	-1	N/A	-1	-1	-1	null	-1	null	null
1520538346000	000013BC	SignIn	bndsl-161112-371	bnh-mp-01-02	0	3	0		-1	-1	-1	null	3102	null	null
      """
    
    val lines = datesampe.split("\n").filter(x => StringUtil.isNotNullAndEmpty(x))
    
    val sparkSession = SparkSession.builder().appName("LOCAL").master("local[2]").getOrCreate()
    val sc = sparkSession.sparkContext
    //sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    //sc.hadoopConfiguration.set(Parameters.SPARK_READ_DIR_RECURSIVE, "true")
    val conlog = sc.parallelize(lines, 1).map(x => ConLog(x)).filter(x => x != null)
    val rs = ConLogDriver.groupBrasByMinutes(sparkSession, conlog).collect()
    
    val test1 = rs.find(x => x.bras == "BNH-MP-01-02" && x.date == "2018-03-09 02:36:00").get
    assert(test1.signInUnique == 3)
    assert(test1.signIn == 3)
    assert(test1.logoffUnique == 19)
    assert(test1.logoff == 19)
    
    val test2 = rs.find(x => x.bras == "BNH-MP-01-02" && x.date == "2018-03-09 02:45:00" && x.card == 3).get
    assert(test2.signInUnique == 1)
    assert(test2.signIn == 1)
    assert(test2.logoffUnique == 0)
    assert(test2.logoff == 0)
    
    //rs.foreach(println)
  }
}
