package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.flink.testutil._
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
}
