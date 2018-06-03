
name := "prj_04_EL_F2FRT"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "com.typesafe" % "config" % "1.3.2",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "6.1.0.jre8" % "provided"
)