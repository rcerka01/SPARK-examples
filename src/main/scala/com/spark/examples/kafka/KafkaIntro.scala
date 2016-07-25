package com.spark.examples.kafka

import java.util.regex.Matcher

import com.spark.examples.util.Utilities._
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}


// Start Zookeeper
//   zookeeper-server-start.sh /usr/local/etc/kafka/zookeeper.properties
// Start Kafka (on a new terminal)
//   kafka-server-start.sh /usr/local/etc/kafka/server.properties

// on new terminal
// create topic
//   kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 -partitions 1 --topic Hello-Kafka
// returns:  Created topic "Hello-Kafka"
// to see topics in cafca:
//   kafka-topics.sh --list --zookeeper localhost:2181

// Start Producer to Send Messages
//   Produce from console:
//     kafka-console-producer.sh --broker-list localhost:9092 --topic Hello-Kafka
//   Produce from file:
//     kafka-console-producer.sh --broker-list localhost:9092 --topic Hello-Kafka < /Users/raitis/Desktop/log.txt
// Start Receiver (on new terminal) (no need for KafkaIntro example)
//   kafka-console-consumer.sh --zookeeper localhost:2181 —topic Hello-Kafka --from-beginning

/**
  * Created by raitis on 19/07/2016.
  */
object KafkaIntro extends App {

  val streamingContext = new StreamingContext("local[*]", "KafkaExample", Seconds(1))

  setupLogging()

  val pattern = apacheLogPattern()

  // set Kafka
  val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
  val topics = Set("Hello-Kafka")

  // recieve
  val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder] (
    streamingContext,
    kafkaParams,
    topics
  ) map ( _._2 ) // (tpicName, message) so we need just a message

  // organise (request is 5th element in utilities Array)
  val requests = lines map ( x => {
    val matcher: Matcher = pattern.matcher(x)
    if (matcher.matches()) matcher.group(5)
  })

  val urls = requests map ( x => {
    val arr = x.toString.split(" ")
    if (arr.size == 3) arr(1) else "error"
  })

  // reduce (5 min win, 1 min slide)
  val urlCounts = urls.map( x => (x, 1)).reduceByKeyAndWindow(_+_, _-_, Seconds(300), Seconds(1))

  // sort and print
  val sorted = urlCounts.transform( rdd => rdd.sortBy( x => x._2, false) )
  sorted.print()

  streamingContext.checkpoint("/Users/raitis/Dropbox/scala/AppWorkspace/SPARK-intro/checkpoint")
  streamingContext.start()
  streamingContext.awaitTermination()
}
