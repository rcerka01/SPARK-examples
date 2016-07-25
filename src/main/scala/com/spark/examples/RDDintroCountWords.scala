package com.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by raitis on 15/07/2016.
  */
object RDDintroCountWords extends App {

  val conf = new SparkConf()
    .setAppName("RDDintro")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)
  val input = sc.textFile("book.txt")

//  val word = input flatMap (word => word.split(' '))
//  val lowerCaseWords = word map (lcWord => lcWord.toLowerCase())

  val lowerCaseWords = for {
    text <- input
    word <- text.split(' ')
  } yield word toLowerCase()

  val count = lowerCaseWords countByValue()
  val firstTen = count.take(10)

  // print a map
  for ((name, count) <- firstTen ) {
    println(name + ": " + count)
  }

  sc.stop()

}
