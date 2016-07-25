package com.spark.examples.flume

import java.util.regex.Matcher

import com.spark.examples.util.Utilities._
import org.apache.spark.streaming.flume._
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by raitis on 23/07/2016.
  */

// add extra sink into Flume and pool data from that. It will work as a buffer, therefore no data go lost
// FlumePullIntro:
//   flume-ng agent --conf conf --conf-file /usr/local/Cellar/flume/1.6.0/conf/spark-pull-based.conf --name a1 -Dflume.root.logger=INFO,console
// (run first Flume, ignore errors)


object FlumePullIntro extends App {

  val streamingContext = new StreamingContext("local[*]", "FlumePullExample", Seconds(1))

  setupLogging()

  val pattern = apacheLogPattern()

  val flumeStream = FlumeUtils.createPollingStream(streamingContext, "localhost", 9192)

  val lines = flumeStream map ( x => new String(x.event.getBody.array()))

  val requests = lines map ( x => {
    val matcher: Matcher = pattern.matcher(x)
    if (matcher.matches()) matcher.group(5)
  })

  val urls = requests map ( x => {
    val arr = x.toString.split(" ")
    if (arr.size == 3) arr(1) else "error"
  })

  val urlCounts = urls.map ( x => (x, 1) ).reduceByKeyAndWindow( _+_, _-_, Seconds(300), Seconds(1) )

  val sortedresults = urlCounts.transform( rdd => rdd.sortBy( x => x._2, false ) )
  sortedresults.print()

  streamingContext .checkpoint("/Users/raitis/Dropbox/scala/AppWorkspace/SPARK-intro/checkpoint")
  streamingContext .start()
  streamingContext .awaitTermination()


}


// Flume conf:
//  a1.sources = r1
//  a1.sinks = s1
//  a1.channels = c1
//
//  a1.sources.r1.type = exec
//  # in real life it should be stream to port with netcat
//  # or consume only added new data to file
//  a1.sources.r1.command = cat /Users/raitis/Desktop/log.txt
//  a1.sources.r1.channels = c1
//
//  a1.sinks.s1.type = org.apache.spark.streaming.flume.sink.SparkSink
//  a1.sinks.s1.hostname = localhost
//  a1.sinks.s1.port = 9192
//  a1.sinks.s1.channel = c1
//
//  a1.channels.c1.type = memory
//  # play arround capacity pass to sink (1000 / 100 was, sort of, to low)
//  a1.channels.c1.capacity = 50000
//  a1.channels.c1.transactionCapacity = 1000
//
//  a1.sources.r1.channels = c1
//  a1.sinks.s1.channel = c1