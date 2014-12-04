name := "SparkTest"

version := "1.0"

scalaVersion := "2.10.4"

assemblyJarName in assembly := "something.jar"

test in assembly := {}

mainClass in assembly := Some("com.example.Main")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.1.0",
  "org.apache.spark" %% "spark-sql" % "1.1.0")