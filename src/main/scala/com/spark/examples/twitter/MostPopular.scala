package com.spark.examples.twitter

import com.spark.examples.util.Utilities
import Utilities._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

// kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic testLogs

/**
  * Created by raitis on 16/07/2016.
  */
object MostPopular extends App {

  setupTwitter()

  val streamingContext = new StreamingContext("local[*]", "SaveTweets", Seconds(1)) // batch interval

  setupLogging()

  val tweets = TwitterUtils.createStream(streamingContext, None)

  val tweetText = tweets map (txt => txt.getText)

  val words = tweetText flatMap (word => word.split(' '))

  val hashTags = words filter (word => word.startsWith("#"))
  // val hashTags = words

  val hashTagKeyValues = hashTags map (tag => (tag, 1))

  // or reduce by window
  val hashTagCounts = hashTagKeyValues.reduceByKeyAndWindow((x,y) => x+y, (x,y)=> x-y, Seconds(15), Seconds(5)) // window and slide interval

  val sortedResults = hashTagCounts.transform(rdd => rdd.sortBy(x => x._2, false))

  sortedResults.print


  streamingContext.checkpoint("/Users/raitis/Dropbox/scala/AppWorkspace/SPARK-intro/checkpoint")
  streamingContext.start()
  streamingContext.awaitTermination()

}
