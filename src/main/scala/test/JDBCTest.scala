package test


import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import org.apache.spark.sql.SQLContext
import storage.postgres.PostgresIO
/**
  * Created by hungdv on 01/09/2017.
  */
object JDBCTest {
  def main(args: Array[String]) {
    val jdbc = PostgresIO.getJDBCUrl(Map(    "jdbcUsername" ->"dwh_noc",
    "jdbcPassword" ->"bigdata",
    "jdbcHostname" ->"172.27.11.153",
    "jdbcPort"     ->"5432",
    "jdbcDatabase" ->"dwh_noc"))
    val index = "HNIP51701GC57/0/5/96"
    val date_time = "2017-09-01 00:08:33"
    // connect to the database named "mysql" on the localhost
    /*val conn = DriverManager.getConnection(jdbc)
    val st: PreparedStatement = conn.prepareStatement(s"SELECT host_endpoint ,Sum(cpe_error) as cpe_error ," +
      s"Sum(lostip_error) as  lostip_error FROM dwh_inf_index " +
      s"WHERE host_endpoint = ? AND date_time >= '${date_time}' GROUP BY host_endpoint ")
    st.setString(1,index)*/
    //st.setT(2,date_time)
    val rs = getResult(index,date_time)
    rs match  {
      case Some(rs) =>
        while(rs.next()){
        println(rs.getString("host_endpoint"))
        println(rs.getString("cpe_error"))
        }
      case None => println("Error occur")
    }

  }

  def getResult(index: String, date_time: String): Option[ResultSet] ={
    val jdbc = PostgresIO.getJDBCUrl(Map(    "jdbcUsername" ->"dwh_noc",
      "jdbcPassword" ->"bigdata",
      "jdbcHostname" ->"172.27.11.153",
      "jdbcPort"     ->"5432",
      "jdbcDatabase" ->"dwh_noc"))
    val conn = DriverManager.getConnection(jdbc)
    try{

      val st: PreparedStatement = conn.prepareStatement(s"SELECT host_endpoint ,Sum(cpe_error) as cpe_error ," +
        s"Sum(lostip_error) as  lostip_error FROM dwh_inf_index " +
        s"WHERE host_endpoint = ? AND date_time >= '${date_time}' GROUP BY host_endpoint ")
        st.setString(1,index)
      //st.setT(2,date_time)
      val rs: ResultSet = st.executeQuery()
      Some(rs)
    } catch{
      case e: SQLException => {System.err.print("SQL Exception: " + e); None}
      case e: Exception => {System.err.print(e); None}
      case _ => None
    }
    finally {
      conn.close
    }
  }
}
