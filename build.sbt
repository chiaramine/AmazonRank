name := "HelloScala"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "2.4.0"


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "net.liftweb" %% "lift-webkit" % "3.3.0"


