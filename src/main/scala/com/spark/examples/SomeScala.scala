package com.spark.examples

/**
  * Created by raitis on 14/07/2016.
  */
object SomeScala extends App {

  println(
  {
    val x=1
    x+2
    x+3
  }
  )

  val y = ("y1", "y2", "y3", "y4", "y")
  println(y._5)


  val x = "x1" -> true
  println(x._1)
  println(x._2)

  val l = (1 to 20).toList
  //  val l = List.range(1, 20)
  //  val l = (for (x <- 1 to 20) yield x).toList
  //
  //  val ll = l filter (i => i%3 == 0)
  val ll = l filter (_ % 3 == 0)
  println(ll)

//  case class Foo
//  case class bar
//  case class baz
//
//  def compute2(maybeFoo: Option[Foo]): Option[Int] =
//
//    maybeFoo.flatMap { foo => foo.bar.flatMap { bar => bar.baz.map { baz => baz.compute }}}
//
//  for {
//    foo <- maybeFoo
//    bar <- foo.bar
//    baz <- bar.baz
//  } yield baz.compute

}
