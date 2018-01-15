package streaming_jobs.noc_jobs

import java.sql.Timestamp
import java.util.Properties

import core.streaming.NocParserBroadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods.parse
import parser.{NocLogLineObject, NocParser}
import storage.es.ElasticSearchDStreamWriter._
import org.apache.spark.sql.functions._

import scala.concurrent.duration.FiniteDuration
import org.apache.spark.sql.cassandra._
import storage.postgres.PostgresIO
/**
  * Created by hungdv on 06/07/2017.
  */
object ParseAndSaveNoc {

  def parseAndSave(ssc: StreamingContext,ss: SparkSession,kafkaMessages: DStream[String],nocParser: NocParser,
                   postgresConfig: Map[String,String]): Unit ={
    import scala.language.implicitConversions
    implicit def finiteDurationToSparkDuration(value: FiniteDuration): org.apache.spark.streaming.Duration =
      new Duration(value.toMillis)
    import ss.implicits._
    val sc = ss.sparkContext
    val jdbcUrl = PostgresIO.getJDBCUrl(postgresConfig)
    println(jdbcUrl)
    val bJdbcURL = sc.broadcast(jdbcUrl)
    val pgProperties    = new Properties()
    pgProperties.setProperty("driver","org.postgresql.Driver")
    val bPgProperties   = sc.broadcast(pgProperties)
    val bParser = NocParserBroadcast.getInstance(sc,nocParser)
    val bErrorLevel = sc.broadcast(Predef.Map("USER-3" -> "info",
      "DAEMON-3-RPD_SCHED_SLIP" -> "info",
      "DAEMON-3-PPMD_WRITE_ERROR" -> "info",
      "DAEMON-3-CHASSISD_UNSUPPORTED_FPC" -> "info",
      "DAEMON-4-RPD_MPLS_LSP_BANDWIDTH_CHANGE" -> "info",
      "DAEMON-3-VRRPD_VIP_COUNT_MISMATCH" -> "info",
      "DAEMON-4-RPD_RSVP_NBRUP" -> "info", "DAEMON-3-RPD_DYN_CFG_GET_PROF_NAME_FAILED" -> "info", "LOG NAME" -> "LEVEL", "DAEMON-4-RPD_L2VPN_REMOTE_SITE_COLLISION" -> "info",
      "DAEMON-3-CHASSISD_FRU_UNRESPONSIVE" -> "info", "DAEMON-3-CHASSISD_IPC_MSG_FRU_NOT_FOUND" -> "info", "USER-3-DH_SVC_MULTICAST_FAILURE" -> "critical", "DAEMON-4-RPD_RSVP_BYPASS_DOWN" -> "info",
      "DAEMON-4-CLKSYNCD_FAULT_LOS_SET" -> "critical", "DAEMON-4-DCD_PARSE_ERROR_SCHEDULER" -> "info", "DAEMON-3-BGP_UNUSABLE_ASPATH" -> "info", "DAEMON-3-LIBJNX_REPLICATE_RCP_ERROR" -> "critical",
      "DAEMON-3-RPD_SCHED_SLIP_KEVENT" -> "info", "DAEMON-4-RPD_ISIS_LSPCKSUM" -> "info", "CONFLICT-4-DCD_PARSE_WARN_INCOMPATIBLE_CFG" -> "critical", "DAEMON-3-AUDITD_RADIUS_REQ_TIMED_OUT" -> "info",
      "DAEMON-3-UI_DBASE_OPEN_FAILED" -> "info", "USER-3-DH_SVC_SENDMSG_FAILURE" -> "critical", "DAEMON-3-SPD_CONN_FAILURE" -> "info", "DAEMON-3-CHASSISD_FRU_UNRESPONSIVE_RETRY" -> "info",
      "DAEMON-4-DDOS_PROTOCOL_VIOLATION_CLEAR" -> "critical", "DAEMON-4-AUTHD_AUTH_SERVER_STATUS_CHANGE" -> "critical", "PFE-4" -> "info", "DAEMON-3-RPD_DYN_CFG_PROCESSING_FAILED" -> "info",
      "DAEMON-3-SNMPD_SEND_FAILURE" -> "info", "DAEMON-3-COSD_GENCFG_WRITE_FAILED" -> "info", "INTERACT-3-UI_COMMIT_ROLLBACK_FAILED" -> "critical", "DAEMON-4-L2ALD_MAC_MOVE_NOTIFICATION" -> "critical",
      "DAEMON-3-CHASSISD_POWER_CHECK" -> "info", "DAEMON-3-JTASK_SCHED_SLIP_KEVENT" -> "info", "DAEMON-3-RPD_DYN_CFG_RESPONSE_SLOW" -> "info", "USER-3-LIBJNX_SOCKET_FLUSH_TIMEOUT" -> "critical",
      "DAEMON-4-RPD_MPLS_LSP_CHANGE" -> "info", "DAEMON-3-UI_CHILD_SIGNALED" -> "info", "DAEMON-3-MIB2D_RTSLIB_READ_FAILURE" -> "info", "DAEMON-4-L2ALD_MAC_LIMIT_RESET_IF" -> "info",
      "DAEMON-4-CHASSISD_PEM_INPUT_BAD" -> "info", "DAEMON-3-PS_FAILURE" -> "info", "DAEMON-3-JTASK_SNMP_CONN_RETRY" -> "info", "AUTH-3" -> "info", "DAEMON-4-UI_REBOOT_EVENT" -> "info",
      "DAEMON-3-RPD_SNMP_CONN_RETRY" -> "info", "DAEMON-4-RPD_MPLS_PATH_UP" -> "critical", "DAEMON-4-L2ALD_MAC_LIMIT_RESET_BD" -> "info", "DAEMON-4-BGP_NLRI_MISMATCH" -> "info",
      "USER-4-LIBJSNMP_NS_LOG_WARNING" -> "critical", "DAEMON-4-DDOS_PROTOCOL_VIOLATION_SET" -> "critical", "DAEMON-4-CHASSISD_IPC_WRITE_ERR_NULL_ARGS" -> "info", "DAEMON-3-CHASSISD_VERSION_MISMATCH" -> "info",
      "DAEMON-3-LICENSE" -> "info", "DAEMON-3" -> "info", "AUTH-3-LOGIN_PAM_USER_UNKNOWN" -> "info", "DAEMON-4-UI_MASTERSHIP_EVENT" -> "info", "DAEMON-3-COSD_SCHEDULER_MAP_CONFLICT" -> "info",
      "AUTH-3-LOGIN_PAM_ERROR" -> "info", "DAEMON-4-DDOS_SCFD_FLOW_AGGREGATED" -> "info", "DAEMON-3-DCD_PARSE_EMERGENCY" -> "emergency", "DAEMON-3-SNMPD_TRAP_WARM_START" -> "critical",
      "CONFLICT-4-RPD_CFG_TRACE_FILE_MISSING" -> "info", "DAEMON-4-L2ALD_MAC_MOVE_NOTIF_EXTENSIVE" -> "critical", "SYSLOG-3" -> "info", "DAEMON-4-RPD_SCHED_MODULE_LONGRUNTIME" -> "critical",
      "DAEMON-3-BGP_WRITE_WOULD_BLOCK" -> "info", "DAEMON-4-SNMP_TRAP_LINK_DOWN" -> "info", "NTP-3" -> "info", "DAEMON-3-RPD_MPLS_OAM_PING_REPLY_TIMEOUT" -> "critical", "DAEMON-4-VRRPD_NEW_BACKUP" -> "info",
      "DAEMON-4-CHASSISD_CONFIG_CHANGE_IFDEV_DEL" -> "info", "USER-3-EVENTD_CONFIG_CHANGE_SUCCESS" -> "info", "DAEMON-4-RPD_SCHED_CUMULATIVE_LONGRUNTIME" -> "critical", "INTERACT-4-UI_RESTART_FAILED_EVENT" -> "info",
      "DAEMON-4-RPD_RSVP_BACKUP_DOWN" -> "critical", "DAEMON-4-BGP_ADDR_NOT_FOUND" -> "critical", "DAEMON-3-MIB2D_SNMP_INDEX_ASSIGN" -> "critical", "DAEMON-4-RPD_MPLS_LSP_SWITCH" -> "info", "DAEMON-3-SNMPD_SOCKET_FAILURE" -> "critical",
      "INTERACT-4-UI_RESTART_EVENT" -> "info", "DAEMON-3-CHASSISD_I2CS_READBACK_ERROR" -> "info", "DAEMON-4-L2ALD_MAC_LIMIT_REACHED_IFBD" -> "critical", "DAEMON-3-UI_WRITE_LOSTCONN" -> "info", "AUTH-3-LOGIN_PAM_AUTHENTICATION_ERROR" -> "info",
      "DAEMON-4-DCD_PARSE_ERROR_HIER_SCHEDULER" -> "info", "KERN-4" -> "info", "DAEMON-3-COSD_TX_QUEUE_RATES_TOO_HIGH" -> "critical", "DAEMON-3-LIBJSNMP_NS_LOG_ERR" -> "info",
      "DAEMON-4-LIBJSNMP_NS_LOG_WARNING" -> "critical","DAEMON-3-RPD_RA_DYN_CFG_SES_ID_MISMATCH" -> "info", "DAEMON-3-MIB2D_COUNTER_DECREASING" -> "info",
      "DAEMON-4-RPD_RSVP_BYPASS_UP" -> "info", "USER-4-DH_SVC_ROUTE_ADDITION_FAILURE" -> "info", "DAEMON-3-AUDITD_RADIUS_REQ_DROPPED" -> "critical", "DAEMON-4-RPD_SCHED_CALLBACK_LONGRUNTIME" -> "info",
      "DAEMON-3-CHASSISD_IPC_CONNECTION_DROPPED" -> "info", "DAEMON-4-BFDD_TRAP_SHOP_STATE_DOWN" -> "critical", "DAEMON-4-VRRPD_NEW_MASTER" -> "info", "DAEMON-4-BFDD_TRAP_STATE_DOWN" -> "critical",
      "DAEMON-4-BGP_RESET_PENDING_CONNECTION" -> "critical", "KERN-3-IF_MSG_IFD_BULK_ALARM" -> "info", "DAEMON-3-JTASK_SCHED_SLIP" -> "info", "USER-3-EVENTD_CONFIG_CHANGE_FAILED" -> "critical",
      "DAEMON-3-CHASSISD_FAN_FAILURE" -> "info", "DAEMON-3-SPD_CONN_OPEN_FAILURE" -> "info", "USER-3-LIBJNX_EXEC_FAILED" -> "critical", "DAEMON-4-RPD_RSVP_NBRDOWN" -> "info", "DAEMON-3-UI_CHILD_WAITPID" -> "info",
      "DAEMON-4-DCD_PARSE_WARN_IDENTICAL_SUBNET" -> "info", "PFE-3" -> "info", "DAEMON-3-MIB2D_RTSDB_DOWN" -> "info", "CONFLICT-4-RPD_PLCY_CFG_PREFIX_LEN_SHORT" -> "info", "INTERACT-4-UI_COMMIT_COMPLETED" -> "info",
      "DAEMON-3-L2ALD_MAC_LIMIT_REACHED_BD" -> "critical", "DAEMON-3-CMLC" -> "info", "DAEMON-3-BGP_CONNECT_FAILED" -> "info", "DAEMON-4-COSD_CLASS_8021P_UNSUPPORTED" -> "info",
      "DAEMON-4-RPD_BGP_NEIGHBOR_STATE_CHANGED" -> "critical", "DAEMON-4-RPD_ISIS_LDP_SYNC" -> "emergency", "DAEMON-3-WARNING" -> "info", "DAEMON-4-RPD_MPLS_LSP_DOWN" -> "info",
      "DAEMON-3-KCOM_LIB_ERROR" -> "info", "DAEMON-4-RPD_MPLS_PATH_DOWN" -> "critical", "CONFLICT-4-RPD_VPLS_INTF_NOT_IN_SITE" -> "info", "DAEMON-3-RPD_L2VPN_SITE_COLLISION" -> "critical",
      "DAEMON-4" -> "info", "DAEMON-4-CHASSISD_PEM_TEMPERATURE" -> "emergency", "DAEMON-4-JTASK_CFG_CALLBACK_LONGRUNTIME" -> "critical", "USER-4-UI_CLI_MMAP_INCREASE" -> "info", "CONFLICT-4" -> "info",
      "DAEMON-3-CHASSISD_FRU_ONLINE_TIMEOUT" -> "info", "DAEMON-4-BGP_PREFIX_THRESH_EXCEEDED" -> "emergency", "CONFLICT-4-PARSE_WARN_NO_ROUTER_AD_CFG" -> "info", "DAEMON-4-L2ALD_DEFAULT_VLAN_DISABLED" -> "info",
      "DAEMON-3-RPD_PPM_WRITE_ERROR" -> "info", "DAEMON-3-DCD_CONFIG_WRITE_FAILED" -> "critical", "DAEMON-4-RPD_KRT_KERNEL_BAD_ROUTE" -> "info", "KERN-3" -> "info", "DAEMON-4-COSD_OUT_OF_DEDICATED_QUEUES" -> "critical",
      "DAEMON-3-VRRPD_MISSING_VIP" -> "info", "DAEMON-4-RPD_MPLS_REQ_BW_NOT_AVAILABLE" -> "info", "DAEMON-3-CHASSISD_FASIC_HSL_LINK_ERROR" -> "info", "DAEMON-3-UI_RECONN_READ_FAILED" -> "info",
      "DAEMON-3-COSD_AGGR_CONFIG_INVALID" -> "critical", "DAEMON-3-UI_CONFIGURATION_ERROR" -> "info", "DAEMON-4-RPD_MPLS_LSP_UP" -> "info", "KERN-3-KERN_ARP_DUPLICATE_ADDR" -> "critical",
      "KERN-3-IF_MSG_IFD_BULK_MUP_DOWN" -> "info", "DAEMON-3-IFDE" -> "info", "DAEMON-4-JTASK_CFG_SCHED_CUMU_LONGRUNTIME" -> "critical", "DAEMON-3-RPD_KRT_Q_RETRIES" -> "info"))
    val lines: DStream[NocLogLineObject] = kafkaMessages.transform(extractMessageAndValue("message", bParser))
    val lookupErrorLevel: (String => String) = (arg: String) =>{
      bErrorLevel.value.getOrElse(arg,"N/A")
    }
    val sqlLookup = org.apache.spark.sql.functions.udf(lookupErrorLevel)

    /*val currentTimeStamp:(String => java.sql.Timestamp) = (args: String) => {
      val time = new Timestamp(System.currentTimeMillis())
      time
    }
    val sqlJavaTimeStamp = org.apache.spark.sql.functions.udf(currentTimeStamp)
    */
    //Save To ES:
    //
    // lines.persistToStorageDaily(Predef.Map[String, String]("indexPrefix" -> "noc", "type" -> "parsed"))

    lines.foreachRDD{
      (rdd: RDD[NocLogLineObject],time: org.apache.spark.streaming.Time) =>
        //filter from RDD -> sev == wraning or error
        rdd.cache()

        val brasErAndW: DataFrame = rdd.toDF("error_name","pri","bras_id","time","facility","severity")
          .select("bras_id","error_name").cache()
        //TODO : Save to postgres. assync mode.
        val brasErMapping = brasErAndW.withColumn("error_level",sqlLookup(col("error_name")))
          .withColumn("date_time",org.apache.spark.sql.functions.current_timestamp())
          .dropDuplicates("bras_id","date_time")
          .cache()
        //Save kibara error to Postgres.
        try {
          PostgresIO.writeToPostgres(ss,brasErMapping,bJdbcURL.value,"dwh_kibana",SaveMode.Append,bPgProperties.value)

        } catch {
          case e: Exception => println("Exceotion when write data to pg")
          case _ => println("Ignore!")
        }

        val brasErAndWaWithFlag= brasErMapping.withColumn("info_flag",when(col("error_level") === "info",1).otherwise(0))
          .withColumn("critical_flag",when(col("error_level") === "critical",1).otherwise(0))
          .cache()
        brasErMapping.unpersist()

        val brasErrorCount = brasErAndWaWithFlag.groupBy(col("bras_id"),col("date_time"))
          .agg(sum(col("info_flag")).as("total_info_count"),sum(col("critical_flag")).as("total_critical_count"))

        //brasErrorCount.show()
        //Save to Cassandra
        //brasErrorCount.write.mode("append").cassandraFormat("noc_bras_error_counting","radius","test").save()
        //TODO :Save to postgres
        try {
          PostgresIO.writeToPostgres(ss, brasErrorCount, bJdbcURL.value, "dwh_kibana_agg", SaveMode.Append, bPgProperties.value)
        } catch {
          case e: Exception => println("Exceotion when write data to pg")
          case _ => println("Ignore!")
        }
        "Finish batch."
        rdd.unpersist(true)
        brasErAndW.unpersist(true)
        brasErAndWaWithFlag.unpersist(true)

          // CALCULATE BASE ON Severity - warning - error
          // Since versin3 -> calculate by error_name
 /*       val brasErAndW: DataFrame = rdd.filter(line => (line.severity == "warning" || line.severity == "err"))
          .toDF("error","pri","bras_id","time","facility","severity")
          .select("bras_id","severity").cache()

        val brasErAndWaWithFlag= brasErAndW.withColumn("erro_flag",when(col("severity") === "err",1).otherwise(0))
                                           .withColumn("warning_flag",when(col("severity") === "warning",1).otherwise(0))
                                              .cache()

        val brasErrorCount = brasErAndWaWithFlag.groupBy(col("bras_id"))
          .agg(sum(col("erro_flag")).as("total_err_count"),sum(col("warning_flag")).as("total_warning_count"))
          .withColumn("time",org.apache.spark.sql.functions.current_timestamp())

        brasErrorCount.write.mode("append").cassandraFormat("noc_bras_error_counting","radius","test").save()
        rdd.unpersist(true)
        brasErAndW.unpersist(true)
        brasErAndWaWithFlag.unpersist(true)*/
    }

    ///////

  }
  def extractMessageAndValue = (key: String, bParser: Broadcast[NocParser]) => (mesgs: RDD[String]) => mesgs.map { msg =>
    implicit val formats = org.json4s.DefaultFormats
    val value = parse(msg).extract[Map[String, Any]].get(key).getOrElse(null)
    value.asInstanceOf[String]
  }.filter(value => value != null).map { line =>
    val parserObject = bParser.value.extractValue(line.replace("\n", "").replace("\r", "")).getOrElse(None)
    parserObject match {
      case Some(x) => x
      case _ => None
    }
    parserObject
  }.filter(x => x != None).map(ob => ob.asInstanceOf[parser.NocLogLineObject]).filter(x => x.devide != "n/a")

}



object BrasErrorTest{
  def main(args: Array[String]): Unit = {

    val sparkSession  = SparkSession.builder().appName("test_bras_erro").master("local[1]").getOrCreate()
    import sparkSession.implicits._
    val brasErAndW = sparkSession.sparkContext.parallelize(Seq(("INTERACT-6-UI_CMDLINE_READ_LINE","190","NTN-MP01-2","07:00:30","local7","info"),
      ("DAEMON-4-DDOS_PROTOCOL_VIOLATION_SET","28","NTN-MP01-2","07:00:30","daemon","warning"),
      ("PFE-4","164","VTU-MP01-1-NEW","07:00:30","local4","err"))).toDF("error","pri","devide","time","facility","severity")

    val brasErAndWaWithFlag= brasErAndW.withColumn("erro_flag",when(col("severity") === "err",1).otherwise(0))
      .withColumn("warning_flag",when(col("severity") === "warning",1).otherwise(0))
      .cache()

    val brasErrorCount = brasErAndWaWithFlag.groupBy(col("devide"))
      .agg(sum(col("erro_flag")).as("total_err_count"),sum(col("warning_flag")).as("total_warning_count"))
      .withColumn("time",org.apache.spark.sql.functions.current_timestamp())
    brasErrorCount.show()
    val currentTimeStamp:(String => java.sql.Timestamp) = (args: String) => {
      val time = new Timestamp(System.currentTimeMillis())
      time
    }
    val sqlJavaTimeStamp = org.apache.spark.sql.functions.udf(currentTimeStamp)

    val brasErrorCount2 = brasErAndWaWithFlag.groupBy(col("devide"))
      .agg(sum(col("erro_flag")).as("total_err_count"),sum(col("warning_flag")).as("total_warning_count"))
      .withColumn("time",sqlJavaTimeStamp(col("devide")))
    brasErrorCount2.show()
  }
}