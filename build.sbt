name := "SPARK-intro"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "1.6.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-twitter" % sparkVersion,

  "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-flume" %sparkVersion,
  "org.apache.spark" %% "spark-streaming-kinesis-asl" % sparkVersion
)

    