ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "StructuredStreamingWithKafka"
  )

libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
 "org.apache.spark" %% "spark-sql" % "3.5.3",
   // https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.3",
     // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
 "org.apache.kafka" % "kafka-clients" % "3.3.2"


)
