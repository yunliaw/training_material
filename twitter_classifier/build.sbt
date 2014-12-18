import AssemblyKeys._ // put this at the top of the file

name := "twitter_classfier"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.1.1",
  "org.apache.spark" %% "spark-mllib" % "1.1.1",
  "org.apache.spark" %% "spark-streaming" % "1.1.1" % "provided",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.1.1",
  "com.google.code.gson" % "gson" % "2.3",
  "org.twitter4j" % "twitter4j-core" % "3.0.3"
)

resourceDirectory in Compile := baseDirectory.value / "resources"

assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
