package com.spark.examples.flume

/**
  * Created by raitis on 21/07/2016.
  */


import java.util.regex.Matcher

import com.spark.examples.util.Utilities._
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.flume._
import org.apache.spark.streaming.{Seconds, StreamingContext}


// Hallo world:
//   flume-ng agent --conf conf --conf-file /usr/local/Cellar/flume/1.6.0/conf/HelloFlume.conf --name a1 -Dflume.root.logger=INFO,console
// FlumePushIntro:
//   flume-ng agent --conf conf --conf-file /usr/local/Cellar/flume/1.6.0/conf/spark-push-based.conf --name a1 -Dflume.root.logger=INFO,console

// spark-push-based.conf
//   a1.sources=r1
//   a1.sinks=avroSink
//   a1.channels=c1
//
//   a1.sources.r1.type=exec
//   #a1.sources.r1.command=tail -F /Users/raitis/Desktop/log.txt
//   a1.sources.r1.command=cat /Users/raitis/Desktop/log.txt
//   a1.sources.r1.channels=c1
//
//   a1.sinks.avroSink.type = avro
//   a1.sinks.avroSink.channel = c1
//   a1.sinks.avroSink.hostname = localhost
//   a1.sinks.avroSink.port = 9191
//
//   a1.channels.c1.type=memory
//   a1.channels.c1.capacity=1000
//   a1.channels.c1.transactionCapacity=100
//
//   #a1.sources.r1.channels=c1
//   #a1.sinks.avroSink.channel=c1

object FlumePushIntro extends App {

  // Create the context with a 1 second batch size
  val ssc = new StreamingContext("local[*]", "FlumePushExample", Seconds(1))

  setupLogging()

  // Construct a regular expression (regex) to extract fields from raw Apache log lines
  val pattern = apacheLogPattern()

  // Create a Flume stream receiving from a given host & port. It's that easy.
  val flumeStream = FlumeUtils.createStream(ssc, "localhost", 9191)

  val lines = flumeStream.map(x => new String(x.event.getBody().array()))

  // Extract the request field from each log line
  val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})

  // Extract the URL from the request
  val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})

  // Reduce by URL over a 5-minute window sliding every second
  val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

  // Sort and print the results
  val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
  sortedResults.print()

  // Kick it off
  ssc.checkpoint("/Users/raitis/Dropbox/scala/AppWorkspace/SPARK-intro/checkpoint")
  ssc.start()
  ssc.awaitTermination()

}



