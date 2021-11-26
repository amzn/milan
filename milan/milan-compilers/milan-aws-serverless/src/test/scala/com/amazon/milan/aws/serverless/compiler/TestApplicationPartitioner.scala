package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.compiler.scala.testing.IntRecord
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang._
import com.amazon.milan.testing.applications._
import org.junit.Assert._
import org.junit.Test


@Test
class TestApplicationPartitioner {
  @Test
  def test_ApplicationPartitioner_PartitionApplication_WithNoStatefulOperations_ReturnsOriginalGraph(): Unit = {
    val input = Stream.of[IntRecord].withId("input")
    val output = input.map(r => IntRecord(r.i + 1))

    val streams = StreamCollection.build(output)

    val config = new ApplicationConfiguration()
    config.setListSource(input, IntRecord(1))

    val instance = new ApplicationInstance(new Application(streams), config)

    val partitioned = ApplicationPartitioner.partitionApplication(instance)

    assertEquals(1, partitioned.size)
    assertEquals(instance.application.streams, partitioned.head.application.streams)
    assertEquals(instance.config, partitioned.head.config)
  }

  @Test
  def test_ApplicationPartitioner_PartitionApplication_WithOneJoinOperations_ReturnsOriginalGraph(): Unit = {
    val left = Stream.of[IntRecord].withId("left")
    val right = Stream.of[IntRecord].withId("right")
    val output = left.leftJoin(right).where((l, r) => l.i == r.i).select((l, r) => l)

    val streams = StreamCollection.build(output)

    val config = new ApplicationConfiguration()
    config.setListSource(left, IntRecord(1))
    config.setListSource(right, IntRecord(1))

    val instance = new ApplicationInstance(new Application(streams), config)

    val partitioned = ApplicationPartitioner.partitionApplication(instance)

    assertEquals(1, partitioned.size)
    assertEquals(instance.application.streams, partitioned.head.application.streams)
    assertEquals(instance.config, partitioned.head.config)
  }

  @Test
  def test_ApplicationPartitioner_PartitionApplication_WithTwoJoinOperations_PutsEachJoinInASeparateGraph(): Unit = {
    val input1 = Stream.of[IntRecord].withId("input1")
    val input2 = Stream.of[IntRecord].withId("input2")
    val input3 = Stream.of[IntRecord].withId("input3")
    val intermediate = input1.leftJoin(input2).where((l, r) => l.i == r.i).select((l, r) => l).withId("intermediate")
    val output = intermediate.leftJoin(input3).where((l, r) => l.i == r.i).select((l, r) => l).withId("output")

    val streams = StreamCollection.build(output)

    val config = new ApplicationConfiguration()
    config.setListSource(input1, IntRecord(1))
    config.setListSource(input2, IntRecord(1))
    config.setListSource(input3, IntRecord(1))

    val instance = new ApplicationInstance(new Application(streams), config)

    val partitioned = ApplicationPartitioner.partitionApplication(instance)

    assertEquals(2, partitioned.size)
    assertAtMostOneStatefulOperationPerPartition(partitioned)
  }

  private def assertAtMostOneStatefulOperationPerPartition(partitions: List[ApplicationInstance]): Unit = {
    partitions.foreach(partition => this.assertAtMostOneStatefulOperation(partition.application.streams))
  }

  private def assertAtMostOneStatefulOperation(streams: StreamCollection): Unit = {
    assertTrue(streams.streams.count(_.stateful) <= 1)
  }
}
