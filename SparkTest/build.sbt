name := "SparkTest"

version := "1.0"

scalaVersion := "2.10.4"

assemblyJarName in assembly := "something.jar"

test in assembly := {}

mainClass in assembly := Some("com.example.Main")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1",
  "org.apache.spark" %% "spark-sql" % "1.3.1",
  "org.apache.spark" %% "spark-hive" % "1.3.1",
  "org.apache.spark" %% "spark-streaming" % "1.3.1",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.3.1")