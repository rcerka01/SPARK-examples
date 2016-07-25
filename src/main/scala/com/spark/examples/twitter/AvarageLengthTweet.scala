package com.spark.examples.twitter

import java.util.concurrent.atomic.AtomicLong

import com.spark.examples.util.Utilities
import Utilities._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by raitis on 16/07/2016.
  */
object AvarageLengthTweet extends App {

  setupTwitter()

  val streamingContext = new StreamingContext("local[*]", "SaveTweets", Seconds(1))

  setupLogging()

  val tweets = TwitterUtils.createStream(streamingContext, None )

  val tweetTxtsLength = tweets.map(tweet => tweet.getText.length)

  // thread safe
  var totalTweets = new AtomicLong(0)
  var totalChars = new AtomicLong(0)
  var longestTweet = new AtomicLong(0)

  tweetTxtsLength.foreachRDD((rdd, time) => {

    val count = rdd.count()

    if (count > 0) {

      totalTweets.getAndAdd(count)
      totalChars.getAndAdd(rdd.reduce((x,y) => x + y))
      longestTweet.set(rdd.max)


      println("Total tweets: " + totalTweets.get() + ", Total chars: " + totalChars.get() + ", Avearage: " + totalChars.get()/totalTweets.get() + ", Longest: " + longestTweet.get())

    }

  })

  streamingContext.start()
  streamingContext.awaitTermination()
}
