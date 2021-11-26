package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.compiler.scala.testing.IntRecord
import com.amazon.milan.compiler.scala.{RuntimeEvaluator, ScalaStreamGenerator}
import com.amazon.milan.graph.StreamCollection
import com.amazon.milan.lang
import com.amazon.milan.program.StreamExpression
import com.amazon.milan.testing.applications._
import org.junit.Assert._
import org.junit.Test

@Test
class TestPartitionedApplication {
  @Test
  def test_PartitionedApplication_ProducesTheSameOutputAsNonPartitionedApplication(): Unit = {
    val input = lang.Stream.of[IntRecord].withId("input")
    val sum = input.sumBy(r => r.i, (_, sum) => IntRecord(sum)).withId("sum")
    val min = sum.minBy(r => r.i).withId("min")

    val streams = StreamCollection.build(min)

    val config = new ApplicationConfiguration()
    config.setListSource(input, IntRecord(1))

    val instance = new ApplicationInstance(new Application(streams), config)

    // Compile the full graph into a function.
    val fullGraphFunc = compile[IntRecord, IntRecord](input.expr, min.expr)

    // Partition the application and compile each partition into a separate function.
    val partitioned = ApplicationPartitioner.partitionApplication(instance)

    assertEquals(2, partitioned.length)

    val streamsPart1 = partitioned(0).application.streams.getDereferencedStreams
    val func1 = compile[IntRecord, IntRecord](
      streamsPart1.find(_.nodeId == "input").get,
      streamsPart1.find(_.nodeId == "sum").get
    )

    val streamsPart2 = partitioned(1).application.streams.getDereferencedStreams
    val func2 = compile[IntRecord, IntRecord](
      streamsPart2.find(_.nodeId == "sum").get,
      streamsPart2.find(_.nodeId == "min").get
    )

    // Run some input through the function compiled from the full graph.
    val inputRecords = List(1, 2, 3, 4).map(i => IntRecord(i)).toStream
    val fullGraphOutput = fullGraphFunc(inputRecords).map(_.i).toArray

    // Compose the partitioned functions and run the same input through.
    val partitionedOutput = func2(func1(inputRecords)).map(_.i).toArray

    assertArrayEquals(fullGraphOutput, partitionedOutput)
  }

  private def compile[TIn, TOut](inputStream: StreamExpression, outputStream: StreamExpression): Stream[TIn] => Stream[TOut] = {
    val functionDef = ScalaStreamGenerator.generateAnonymousFunction(List(inputStream), outputStream)
    RuntimeEvaluator.instance.eval[Stream[TIn] => Stream[TOut]](functionDef)
  }
}
