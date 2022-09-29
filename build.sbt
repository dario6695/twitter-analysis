name := "twitter-analysis"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.2"
val postgresVersion = "42.2.2"
val twitter4jVersion = "4.0.7"
val kafkaVersion = "2.4.0"
val log4jVersion = "2.4.1"
val scalatestVersion = "3.2.11"
val zioVersion = "2.0.2"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(

  //spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,

  // postgres
  "org.postgresql" % "postgresql" % postgresVersion,

  // twitter
  "org.twitter4j" % "twitter4j-core" % twitter4jVersion,
  "org.twitter4j" % "twitter4j-stream" % twitter4jVersion,

  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,

  // kafka
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,

  //zio
  "dev.zio" %% "zio" % zioVersion,

  //test
  "org.scalatest" %% "scalatest" % scalatestVersion % "test",
   "dev.zio" %% "zio-test" % zioVersion % "test",
  "dev.zio" %% "zio-test-sbt" % zioVersion % "test",
  "dev.zio" %% "zio-mock" % "1.0.0-RC8" % "test"

)