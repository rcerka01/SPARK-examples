package com.spark.examples.twitter

/**
  * Created by raitis on 16/07/2016.
  */

import com.spark.examples.util.Utilities
import Utilities._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SaveTweets extends App {

  setupTwitter()

  val streamingContext = new StreamingContext("local[*]", "SaveTweets", Seconds(1))

  setupLogging()

  val tweets = TwitterUtils.createStream(streamingContext, None )

  val tweetTxts = tweets.map(tweet => tweet.getText)

  // tweetTxts.saveAsTextFiles("myTweets", "txt")


  var total: Long = 0

  tweetTxts foreachRDD((rdd, time )=> { // access each rdd

    // if data in the batch
    if (rdd.count()>0) {

      // repartition(1) - consolidate data in one partition
      // cache - insures that same rdd stay in memory for diferent actions
      val repartitionedRDD = rdd.repartition(1).cache()
      repartitionedRDD.saveAsTextFile("MyTweets_" + time.milliseconds.toString)

      total += repartitionedRDD.count()

      println("Tweets: " + total)

      if (total > 1000) {

        System.exit(0)
      }

    }

  })

  streamingContext.start()
  streamingContext.awaitTermination()

}
