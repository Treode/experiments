
name := "conditional-batch-write"

scalaVersion := "2.11.6"

scalaSource in Compile := baseDirectory.value / "scala/main"

scalaSource in Test := baseDirectory.value / "scala/test"

scalacOptions ++= Seq ("-deprecation", "-feature", "-optimize", "-unchecked")

libraryDependencies ++= Seq (
  "net.sf.trove4j" % "trove4j" % "3.0.3",
  "org.scalatest" %% "scalatest" % "2.2.4")
