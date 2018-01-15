package streaming_jobs.dns

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import parser.{AbtractLogLine, AbtractLogParser}

/**
  * Created by hungdv on 31/10/2017.
  */
class DNSParser extends  AbtractLogParser{
  def parse(line: String): (Long,String,String) ={
    val arr = line.split("\t")
    val date_time_cluster = arr(0).split(" ")
    if(date_time_cluster.length == 3){
      (DateTime.parse(date_time_cluster(0),
        DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")).getMillis,arr(2),arr(9))
    }
    else (-1,"er","er")
  }

  def parseRsync(line: String): (Long,String,String) ={
    val arr = line.split("\t")

    if(arr.length == 24){
      (safetyParserStringToLong(arr(0).substring(0,"15094176386151".length).replace(".",""),-1),arr(2),arr(9))
    }
    else (-1,"er","er")
  }
  def toLong(s:String): Option[Long]={
    try{
      Some(s.toLong)
    }
    catch {
      case e:NumberFormatException => None
    }
  }
  def safetyParserStringToLong(aString:String,returnValue:Long) ={
    toLong(aString) match{
      case Some(n) => n
      case None => returnValue
    }
  }

  def extractValues(lines: String) : Option[AbtractLogLine] = {
    null
  }
}
object DNSParserTest{
  def main(args: Array[String]): Unit = {
    val string = "2017-10-31T00:59:25.291Z pig 1509411565.291462\tCOaY9E3vDxT3hsDvKj\t113.22.187.131\t42093\t210.245.31.221\t53\tudp\t10637\t-\tblackjacks.com.au\t1\tC_INTERNET\t15\tMX\t-\t-\tF\tF\tTF\t0\t-\t-\tF"
    val parser = new DNSParser()
    println(parser.parse(string))


    val string2 = "1509728272.255814\tClmq3Y3BkqYhUOP2Li\t118.71.199.65\t52757\t208.67.222.222\t53\tudp\t4200\t-\t56.31.74.52.in-addr.arpa\t-\t-\t-\t-\t0\tNOERROR\tF\tF\tF\tT\t0\tec2-52-74-31-56.ap-southeast-1.compute.amazonaws.com\t241.000000\tF"
    val arr = string2.split("\t")
    //println(arr.length)
    //arr.foreach(print(_ ))
    println(parser.parseRsync(string2))
    println (arr.length)
  }
}