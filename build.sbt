name := "Analytics-For-IoT-Devices-Using-Spark"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.12"
val sparkVersion = "2.4.0"
val slf4jVersion = "1.7.16"
val log4jVersion = "1.2.17"
val sparkCassandraConnectorVersion = "2.4.0"

val sparkAndDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)

libraryDependencies ++= sparkAndDependencies
//  .map(_ % "provided")

libraryDependencies ++= Seq(

  "com.datastax.spark" % "spark-cassandra-connector_2.11" % sparkCassandraConnectorVersion,

  "org.scalatest" %% "scalatest" % "3.0.5",

  "com.typesafe" % "config" % "1.3.3",
  "com.github.scopt" %% "scopt" % "3.7.1"

)
