package streaming_jobs

import org.json4s.jackson.JsonMethods.parse

/**
  * Created by hungdv on 11/05/2017.
  */
object PrefilterTest {
  def jsonStrToMap(jsonStr: String) :Map[String,String] ={

    implicit val formats = org.json4s.DefaultFormats

    parse(jsonStr).extract[Map[String,String]]

  }
  def main(args: Array[String]): Unit = {
    val json  = "{\"message\":\"15:38:26 000002AC Auth-Local:SignIn: agdsl-141004-826, AGG-MP01-1, 72FE4713\",\"@version\":\"1\",\"@timestamp\":\"2017-05-11T08:38:27.326Z\",\"type\":\"isc-radius\",\"host\":\"118.69.241.48\"}"
    val mapOfRawLogObject: Map[String, String] = jsonStrToMap(json)
    val msg: String = mapOfRawLogObject.get("message").getOrElse(null)
    println(msg)
  }
}
