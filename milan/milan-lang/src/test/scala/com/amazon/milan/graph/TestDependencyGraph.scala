package com.amazon.milan.graph

import com.amazon.milan.lang._
import com.amazon.milan.program.ExternalStream
import com.amazon.milan.test.{IntKeyValueRecord, IntRecord}
import org.junit.Assert._
import org.junit.Test


@Test
class TestDependencyGraph {
  @Test
  def test_DependencyGraph_TopologicalSort_WithTwoStreams(): Unit = {
    val stream1 = Stream.of[IntRecord]
    val stream2 = stream1.map(r => IntRecord(r.i + 1))

    val graph = DependencyGraph.build(stream2)

    val sorted = graph.topologicalSort
    assertEquals(List(stream1.expr, stream2.expr), sorted.map(_.expr))
  }

  @Test
  def test_DependencyGraph_TopologicalSort_WithJoinedStreams(): Unit = {
    val stream1 = Stream.of[IntKeyValueRecord]
    val stream2 = Stream.of[IntKeyValueRecord]
    val stream3 = stream1.leftJoin(stream2).where((l, r) => l.key == r.key)
    val stream4 = stream3.select((l, r) => IntKeyValueRecord(l.key, l.value + r.value))

    val graph = DependencyGraph.build(stream4)

    val sorted = graph.topologicalSort.map(_.expr)
    assertEquals(List(stream1.expr, stream2.expr, stream3.expr, stream4.expr), sorted)
  }

  @Test
  def test_DependencyGraph_TopologicalSort_WithSequentialJoins(): Unit = {
    val stream1 = Stream.of[IntKeyValueRecord]
    val stream2 = Stream.of[IntKeyValueRecord]
    val stream3 = stream1.leftJoin(stream2).where((l, r) => l.key == r.key)
    val stream4 = stream3.select((l, r) => IntKeyValueRecord(l.key, l.value + r.value))
    val stream5 = stream2.leftJoin(stream4).where((l, r) => l.key == r.key)
    val stream6 = stream5.select((l, r) => IntKeyValueRecord(l.key, l.value + r.value))

    val graph = DependencyGraph.build(stream6)

    val sorted = graph.topologicalSort.map(_.expr)
    assertEquals(List(stream2.expr, stream1.expr, stream3.expr, stream4.expr, stream5.expr, stream6.expr), sorted)
  }

  @Test
  def test_DependencyGraph_BuildSubGraph_WithThreeSequentialStreams_WithSecondStreamAsInputStream_ThenTopoSort_ReturnsSecondAndThirdStreamsWithSecondStreamReplacedByExternalStream(): Unit = {
    val stream1 = Stream.of[IntRecord]
    val stream2 = stream1.map(r => IntRecord(r.i))
    val stream3 = stream2.map(r => IntRecord(r.i))

    val sub = DependencyGraph.buildSubGraph(Seq(stream2.expr), Seq(stream3.expr))

    val sorted = sub.topologicalSort.map(_.expr)

    // The first item should be stream2 but changed into an ExternalStream.
    val ExternalStream(stream2.expr.nodeId, stream2.expr.nodeName, outputStream2Type) = sorted.head
    assertEquals(stream2.expr.tpe, outputStream2Type)

    assertEquals(2, sorted.length)
    assertEquals(stream3.expr.nodeId, sorted.last.nodeId)
  }

  @Test
  def test_DependencyGraph_TopologicalSort_WithFlatMapOfGroupedStream(): Unit = {
    val input = Stream.of[IntKeyValueRecord].withId("input")
    val grouped = input.groupBy(r => r.key).withId("grouped")

    def sumByValue(s: Stream[IntKeyValueRecord]): Stream[IntKeyValueRecord] =
      s.sumBy(r => r.value, (r, sum) => IntKeyValueRecord(r.key, sum)).withId("sumBy")

    val output = grouped.flatMap((_, group) => sumByValue(group)).withId("output")

    val graph = DependencyGraph.build(output)

    assertEquals(1, graph.rootNodes.length)

    val root = graph.rootNodes.head
    assertEquals("output", root.expr.nodeId)

    assertEquals(1, root.children.length)

    val child1 = root.children.head
    assertEquals(1, child1.children.length)
    assertEquals("grouped", child1.children.head.nodeId)

    assertEquals(1, child1.children.length)
    val child2 = child1.children.head
    assertEquals(None, child2.contextStream)
    assertEquals("input", child2.children.head.nodeId)
  }
}
