package com.amazon.milan.typeutil

import java.time.{Duration, Instant}


package object types {
  val Boolean = new BasicTypeDescriptor[Boolean]("Boolean")
  val Double = new NumericTypeDescriptor[Double]("Double")
  val Float = new NumericTypeDescriptor[Float]("Float")
  val Instant = new BasicTypeDescriptor[Instant]("java.time.Instant")
  val Duration = new BasicTypeDescriptor[Duration]("java.time.Duration")
  val Int = new NumericTypeDescriptor[Int]("Int")
  val Long = new NumericTypeDescriptor[Long]("Long")
  val String = new BasicTypeDescriptor[String]("String")
  val Nothing = new BasicTypeDescriptor[Nothing]("Nothing")
  val EmptyTuple = new TupleTypeDescriptor[Product]("Product", List(), List())
}
