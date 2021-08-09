name := "explore-apache-hudi"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.0"

libraryDependencies += "org.apache.hudi" %% "hudi-spark-bundle" % "0.7.0"

libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"
