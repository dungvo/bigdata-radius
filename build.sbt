
val scalaBuidVersion = "2.11"
val mongoSparkConnectorVerSion = "2.0.0"
val jacksonVersion    = "2.8.4"
val jdbcVersion = "42.0.0"

val kafkaVersion = "0.10.2.0"
val kafkaSparkIntergrationVerison = "2.1.0"

val sparkVersion = "2.0.0"
val ficusVersion = "1.1.2"
val elasticsearchSparkDriverVersion = "6.0.0-alpha-1"
// Move to es spark drive 6.0.0 @since 31-5-2017
//val elasticsearchSparkDriverVersion = "5.0.0-alpha5"
val cassandraSparkConnectorVersion = "2.0.0"
val typeSafeVersion = "1.2.1"
val algebirdVersion = "0.12.0"

val sparkCore = "org.apache.spark" % s"spark-core_${scalaBuidVersion}" % sparkVersion % "provided" excludeAll ExclusionRule(organization = "javax.servlet")
val sparkStreaming = "org.apache.spark" % s"spark-streaming_${scalaBuidVersion}" % sparkVersion
val sparkMlib = "org.apache.spark" % s"spark-mllib_${scalaBuidVersion}" % sparkVersion
val sparkSQL = "org.apache.spark" % s"spark-sql_${scalaBuidVersion}" % sparkVersion

val jacksonCore = "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion
val jacksonDatabind = "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
val jacksonAnnotation = "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion
val postgresDriver  = "org.postgresql" % "postgresql" % jdbcVersion
val jacksonModuleScala = "com.fasterxml.jackson.module" % s"jackson-module-scala_${scalaBuidVersion}" % jacksonVersion
val mongoSparkConnector = "org.mongodb.spark" % s"mongo-spark-connector_${scalaBuidVersion}" % mongoSparkConnectorVerSion

val kafka     = "org.apache.kafka" % "kafka_2.11" % "0.10.2.0"
//val kafka     = "org.apache.kafka" % s"kafka_${scalaBuidVersion}" % kafkaVersion
val kafkaSparkIntergration = "org.apache.spark" % "spark-streaming-kafka-0-10-assembly_2.11" % "2.1.0"
//val kafkaSparkIntergration = "org.apache.spark" % s"spark-streaming-kafka-0-10-assembly_${scalaBuidVersion}" % kafkaSparkIntergrationVerison

val ficus = "net.ceedubs" % s"ficus_${scalaBuidVersion}" % ficusVersion
val cassandraSparkConnector = "com.datastax.spark" % s"spark-cassandra-connector_${scalaBuidVersion}" % cassandraSparkConnectorVersion
val algebirdCore = "com.twitter" % s"algebird-core_${scalaBuidVersion}" % algebirdVersion
val typesafeConfig  = "com.typesafe" % "config" % typeSafeVersion

val elasticsearchSparkConnector = "org.elasticsearch" % s"elasticsearch-spark-20_${scalaBuidVersion}" % elasticsearchSparkDriverVersion



val unit = "junit" % "junit" % "4.4" % "test"
val scalaTest = "org.scalatest" %% "scalatest" % "3.0.0" % "test"
val log4j = "log4j" % "log4j" % "1.2.17" % "provided"
val twitterBijection = "com.twitter" % s"bijection-avro_${scalaBuidVersion}" % "0.8.1"
val gson = "com.google.code.gson" % "gson" % "2.3.1"
val scalajHttp =  "org.scalaj" %% "scalaj-http" % "2.3.0"

lazy val commonSettings = Seq(
  organization := "com.ftel",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.7"
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := "bigdata-radius",
    libraryDependencies ++= Seq(sparkCore,
      sparkStreaming,
      sparkMlib,
      sparkSQL,
      jacksonCore,
      jacksonAnnotation,
      jacksonDatabind,
      postgresDriver,
      jacksonModuleScala,
      mongoSparkConnector,
      kafka,
      kafkaSparkIntergration,
      ficus,
      cassandraSparkConnector,
      algebirdCore,
      typesafeConfig,
      elasticsearchSparkConnector,
      unit,
      scalaTest,
      log4j,
      twitterBijection,
      gson,
      scalajHttp,
      "net.debasishg" % "redisclient_2.10" % "2.11"
      )
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

assemblyExcludedJars in assembly := { 
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName == "scala-library.jar"}
  cp filter {_.data.getName == "junit-3.8.1.jar"}
}

resolvers += Resolver.mavenLocal
