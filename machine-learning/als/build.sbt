import _root_.sbtassembly.Plugin.AssemblyKeys
import AssemblyKeys._
import _root_.sbtassembly.Plugin._

assemblySettings

name := "MovieLensALS"

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.1.1"
