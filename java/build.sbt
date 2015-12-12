name := "Tutorial"

scalaVersion := "2.10.4"


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.2",
  "org.apache.spark" % "spark-streaming_2.10" % "1.5.2",
  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
  "org.twitter4j" % "twitter4j-core" % "3.0.3",
  "org.twitter4j" % "twitter4j-stream" % "3.0.3",
  "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.5.2"
)

mainClass in (Compile, run) := Some("Tutorial")
