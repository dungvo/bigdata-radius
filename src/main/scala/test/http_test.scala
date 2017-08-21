package test

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.log4j.{Level, Logger}

import java.net.URL;
import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import scalaj.http.{Http, HttpOptions, HttpResponse}
/**
  * Created by hungdv on 09/08/2017.
  */
object http_test {

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val login = new Login("noc-mon","noc-mon")
    val loginType = new TypeToken[Login](){}.getType
    val gson = new Gson()
    val loginJson = gson.toJson(login)
    println(loginJson)

    javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(
       new javax.net.ssl.HostnameVerifier(){

        def  verify(hostname: String,
          sslSession: javax.net.ssl.SSLSession ): Boolean =  {
          return true
        }
      });

    val http_url = "https://210.245.0.226/rest/login"
    val porxy = "172.30.45.220"
    val httpRequestResult = Http(http_url)
      //.proxy(porxy,80)
      .postData(loginJson)
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(15000))
      .asString

    println(httpRequestResult.statusLine + httpRequestResult.headers + httpRequestResult.body)
  }
}



/**
  * Created by hungdv on 09/08/2017.
  */

case class Login(username: String, password: String) extends Serializable{}
