package com.amazon.milan.flink.internal

import com.amazon.milan.flink.testing.IntKeyValueRecord
import com.amazon.milan.lang.{Stream, StreamGraph}
import com.amazon.milan.serialization.ScalaObjectMapper
import com.amazon.milan.typeutil.TypeDescriptor
import org.junit.Assert._
import org.junit.Test


@Test
class TestGraphTypeChecker {
  @Test
  def test_GraphTypeChecker_TypeCheckGraph_OfDeserializedGraph_WithFullJoinAndSelect_AssignsExpectedTypes(): Unit = {
    val left = Stream.of[IntKeyValueRecord]
    val right = Stream.of[IntKeyValueRecord]
    val joined = left.fullJoin(right).where((l, r) => l.key == r.key)
    val output = joined.select((l, r) => IntKeyValueRecord(l.key, l.value + r.value))

    val graph = new StreamGraph(output)

    val graphCopy = ScalaObjectMapper.copy(graph).getDereferencedGraph
    GraphTypeChecker.typeCheckGraph(graphCopy)

    val leftCopy = graphCopy.getStream(left.streamId)
    assertEquals(TypeDescriptor.streamOf[IntKeyValueRecord], leftCopy.tpe)

    val rightCopy = graphCopy.getStream(right.streamId)
    assertEquals(TypeDescriptor.streamOf[IntKeyValueRecord], rightCopy.tpe)

    val outputCopy = graphCopy.getStream(output.streamId)
    assertEquals(TypeDescriptor.streamOf[IntKeyValueRecord].fullName, outputCopy.tpe.fullName)
  }

  @Test
  def test_GraphTypeChecker_TypeCheckGraph_OfNestedStreamFunction_DoesntFail(): Unit = {
    val input = Stream.of[IntKeyValueRecord]

    def maxByValue(stream: Stream[IntKeyValueRecord]): Stream[IntKeyValueRecord] = {
      stream.maxBy(r => r.value)
    }

    val output = input.groupBy(r => r.key).flatMap((key, group) => maxByValue(group))

    val graph = new StreamGraph(output).getDereferencedGraph

    GraphTypeChecker.typeCheckGraph(graph)
  }
}
