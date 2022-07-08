name := "spark-big-data"

ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "spark-big-data"
  )

val sparkVersion = "3.3.0"
val vegasVersion = "0.3.11"
val postgresVersion = "42.4.0"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.18.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.18.0",
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion
)