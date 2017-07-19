package util
import scala.collection.mutable
import scala.io
/**
  * Created by hungdv on 10/07/2017.
  */
class StaticFileParsing()  {
  def readAndParseBrasMapping(path: String): mutable.Map[String,String] ={
    val bufferSource = io.Source.fromFile(path)
    val result = scala.collection.mutable.Map[String,String]()
    for (line <- bufferSource.getLines()){

      val cols = line.split(",").map(_.trim)
      val radiusName: String = cols(1)
      val kibanaName = cols(3)
      val opsviewName = cols(2)
      if(opsviewName != "None"){
        if(cols(3) != "None")  {
          result += (("\""+radiusName + "\"")->("\""+ kibanaName +"\""))
        }else{
          result += (("\""+radiusName + "\"")->("\""+ cols(2) +"\""))
        }
      }

    }
    result
  }
  def readAndParseErrorLevel(path: String) : mutable.Map[String,String] = {
      val bufferReader = io.Source.fromFile(path)
      val result = mutable.Map[String,String]()
    for(line <- bufferReader.getLines()){
      val cols = line.split(",").map(_.trim)
      val logName = cols(0)
      if(cols.length >= 3){
        val logType = cols(2)
        result += (("\""+logName + "\"")->("\""+ logType +"\""))
      }else{
        result += (("\""+logName + "\"")->("\""+ "info" +"\""))
      }
    }
    result
  }
  def readAndParseListBras(path: String): mutable.Map[String,String] = {
    val bufferReader = io.Source.fromFile(path)
    val result = mutable.Map[String,String]()
    for(line <- bufferReader.getLines()){
      val cols = line.split(",").map(_.trim)
      val nocName = cols(2)
      if(cols.length >= 4) {
        val radiusName = cols(3)
        result += (("\""+radiusName + "\"")->("\""+ nocName +"\""))
      }
    }
    result
  }
}
object StaticFileTest{
  def main(args: Array[String]): Unit = {
    val path = "/home/hungdv/workspace/bigdata-radius/src/main/resources/bras_final.csv"
    val reader = new StaticFileParsing()
    val resutt = reader.readAndParseBrasMapping(path)
    //println(resutt)
    //println(resutt.size)
    val erroPath = "/home/hungdv/workspace/bigdata-radius/src/main/resources/list_error.csv"
    val error = reader.readAndParseErrorLevel(erroPath)
    println("----------------------------------------")
    //println(error)

    val listbrasPath = "/home/hungdv/workspace/bigdata-radius/src/main/resources/list_bras.csv"
    val bras_Radius_Noc = reader.readAndParseListBras(listbrasPath)
    println(bras_Radius_Noc)
  }
}