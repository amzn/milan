package com.amazon.milan.graph

import com.amazon.milan.lang._
import com.amazon.milan.program.{ExternalStream, StreamMap}
import com.amazon.milan.test.IntRecord
import com.amazon.milan.typeutil._
import org.junit.Assert._
import org.junit.Test


@Test
class TestFlowGraph {
  @Test
  def test_FlowGraph_GetSubGraph_WithLinearGraph_ReturnsFlowGraphWithOnlyDownstreamNodesAndRootExpressionsReplacedWithExternalStream(): Unit = {
    val stream1 = Stream.of[IntRecord].withId("stream1")
    val stream2 = stream1.map(r => IntRecord(r.i + 1)).withId("stream2")
    val stream3 = stream2.map(r => IntRecord(r.i + 1)).withId("stream3")
    val stream4 = stream3.map(r => IntRecord(r.i + 1)).withId("stream4")

    val fullGraph = FlowGraph.build(stream4.expr)
    val subGraph = fullGraph.getSubgraph(List(stream2.expr))

    assertEquals(1, subGraph.rootNodes.length)
    assertEquals(3, subGraph.nodes.size)
    assertEquals(ExternalStream("stream2", "stream2", createTypeDescriptor[IntRecord].toDataStream), subGraph.rootNodes.head.expr)

    val streams = subGraph.toStreamCollection
    val outputStream4 = streams.getDereferencedStreams.filter(_.nodeId == "stream4").head

    outputStream4 match {
      case StreamMap(StreamMap(ExternalStream("stream2", "stream2", _), _), _) => ()
      case _ => assert(false)
    }
  }

  @Test
  def test_FlowGraph_GetSubGraph_WithBranchingBelowNewRoot_ReturnsFlowGraphWithExpectedDownstreamNodes(): Unit = {
    val stream1 = Stream.of[IntRecord].withId("stream1")
    val stream2 = stream1.map(r => r)
    val branch1 = stream2.map(r => r)
    val branch2 = stream2.map(r => r)

    val fullGraph = FlowGraph.build(branch1.expr, branch2.expr)
    val subGraph = fullGraph.getSubgraph(List(stream2.expr))

    assertEquals(1, subGraph.rootNodes.length)
    assertEquals(3, subGraph.nodes.size)
  }
}
