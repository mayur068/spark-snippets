name := "spark-snippets"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "1.6.1",
  "org.apache.spark" %% "spark-core" % "1.6.1"
)