package com.amazon.milan.typeutil

import java.time.{Duration, Instant}


package object types {
  val StreamTypeName: String = "Stream"
  val JoinedStreamsTypeName: String = "JoinedStreams"
  val GroupedStreamTypeName: String = "GroupedStream"

  val Boolean = new BasicTypeDescriptor[Boolean]("Boolean")
  val Double = new NumericTypeDescriptor[Double]("Double")
  val Float = new NumericTypeDescriptor[Float]("Float")
  val Instant = new BasicTypeDescriptor[Instant]("java.time.Instant")
  val Duration = new BasicTypeDescriptor[Duration]("java.time.Duration")
  val Int = new NumericTypeDescriptor[Int]("Int")
  val Long = new NumericTypeDescriptor[Long]("Long")
  val String = new BasicTypeDescriptor[String]("String")
  val Nothing = new BasicTypeDescriptor[Nothing]("Nothing")
  val Unit = new TupleTypeDescriptor[Product]("Product", List(), List())

  def stream(recordType: TypeDescriptor[_]): StreamTypeDescriptor = {
    new DataStreamTypeDescriptor(recordType)
  }

  def joinedStreams(leftRecordType: TypeDescriptor[_], rightRecordType: TypeDescriptor[_]): JoinedStreamsTypeDescriptor = {
    new JoinedStreamsTypeDescriptor(leftRecordType, rightRecordType)
  }

  def groupedStream(recordType: TypeDescriptor[_]): GroupedStreamTypeDescriptor = {
    new GroupedStreamTypeDescriptor(recordType)
  }
}
