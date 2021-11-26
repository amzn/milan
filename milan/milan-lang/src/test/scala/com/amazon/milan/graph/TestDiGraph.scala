package com.amazon.milan.graph

import com.amazon.milan.lang._
import com.amazon.milan.test.IntRecord
import org.junit.Assert._
import org.junit.Test


@Test
class TestDiGraph {
  @Test
  def test_DiGraph_Build_OfOutputExpression_ReturnsGraphIncludingUpstreamExpressions(): Unit = {
    val input = Stream.of[IntRecord]
    val expr1 = input.map(r => IntRecord(r.i + 1))
    val expr2 = expr1.map(r => IntRecord(r.i + 1))

    val graph = DiGraph.build(expr2.expr)

    assertTrue(graph.nodes.exists(_.expr == input.expr))
    assertTrue(graph.nodes.exists(_.expr == expr1.expr))
    assertTrue(graph.nodes.exists(_.expr == expr2.expr))
  }

  @Test
  def test_DiGraph_Partition_WithTwoNodesInDifferentPartitions_ReturnsTwoGraphsWithNoConnectionsBetweenExpressions(): Unit = {
    val input = Stream.of[IntRecord].withId("input")
    val stream1 = input.map(r => IntRecord(r.i + 1)).withId("expr1")
    val stream2 = stream1.map(r => IntRecord(r.i + 1)).withId("expr2")

    val graph = DiGraph.build(stream2.expr)

    val partitionAssignments = Map(stream1.expr -> 1, stream2.expr -> 2)
    val partitions = graph.partition(0, n => partitionAssignments.get(n.expr))

    assertEquals(2, partitions.size)
    val partition1 = partitions(1)

    assertEquals(Set(stream1.expr), partition1.leafNodes.map(_.expr))
    assertEquals(Set(input.expr), partition1.rootNodes.map(_.expr))
    assertEquals(input.expr, partition1.leafNodes.head.expr.getChildren.head)

    val partition2 = partitions(2)
    assertEquals(Set(stream2.expr.nodeId), partition2.leafNodes.map(_.expr.nodeId))
    assertNotEquals(input.expr, partition2.leafNodes.head.expr.getChildren.head)
  }
}
