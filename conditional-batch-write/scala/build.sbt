
name := "conditional-batch-write"

scalaVersion := "2.11.6"

scalaSource in Compile := baseDirectory.value / "src"

scalaSource in Test := baseDirectory.value / "tests"

scalacOptions ++= Seq ("-deprecation", "-feature", "-optimize", "-unchecked")

libraryDependencies ++= Seq (
  "com.lmax" % "disruptor" % "3.2.0",
  "net.sf.trove4j" % "trove4j" % "3.0.3",
  "org.scalatest" %% "scalatest" % "2.2.4",
  "org.jctools" % "jctools-core" % "1.0")

assemblyJarName in assembly := "cbw.jar"

test in assembly := {}
