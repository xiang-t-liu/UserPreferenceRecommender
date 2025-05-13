ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

val spark_version = "3.5.3"

lazy val root = (project in file("."))
  .settings(
    name := "UserPreferenceRecommender"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark_version,
  "org.apache.spark" %% "spark-sql" % spark_version
)

