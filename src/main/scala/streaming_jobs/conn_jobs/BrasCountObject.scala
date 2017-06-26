package streaming_jobs.conn_jobs
import java.sql.Timestamp
/**
  * Created by hungdv on 16/05/2017.
  */
case class BrasCountObject(
                            bras_id: String,
                            signin_total_count :Int,
                            logoff_total_count :Int,
                            signin_distinct_count :Int,
                            logoff_distinct_count :Int,
                            time :java.sql.Timestamp) extends Serializable{

}
case class InfCountObject(
                            host: String,
                            signin_total_count :Int,
                            logoff_total_count :Int,
                            signin_distinct_count :Int,
                            logoff_distinct_count :Int,
                            time :java.sql.Timestamp) extends Serializable{

}
