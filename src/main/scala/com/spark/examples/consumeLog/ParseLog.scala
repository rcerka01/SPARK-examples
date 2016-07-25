package com.spark.examples.consumeLog

import com.spark.examples.util.Utilities._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by raitis on 18/07/2016.
  */

// nc -l -p 1818 < /Users/raitis/Desktop/access_log.txt

object ParseLog extends App {

  val streamingContext = new StreamingContext("local[*]", "ParseLog", Seconds(1)) // batch interval

  setupLogging()

  val pattern = apacheLogPattern()

  val lines = streamingContext.socketTextStream("localhost", 2999, StorageLevel.MEMORY_AND_DISK_SER)

//  val requests = lines.map( line => {
//    val matcher: Matcher = pattern.matcher(line)
//    if (matcher.matches()) matcher.group(5)
//  })
//
//  val urls = requests map ( request => {
//    val arr = request.toString.split(" ")
//    if (arr.size == 3) arr(1)
//    else "error"
//  })
//
//  val urlsCounts = urls.map ( result => (result, 1) ).reduceByKeyAndWindow(_+_, _-_, Seconds(30), Seconds(5))
//
//  val sortedResults = urlsCounts.transform( rdd => {
//    rdd.sortBy(x => x._2, false)
//  })

  streamingContext.checkpoint("/Users/raitis/Dropbox/scala/AppWorkspace/SPARK-intro/checkpoint")
  streamingContext.start()
  streamingContext.awaitTermination()
}
