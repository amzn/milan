package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.sinks.DynamoDbTableSink
import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.compiler.scala.TypeLifter
import com.amazon.milan.compiler.scala.event._
import com.amazon.milan.compiler.scala.testing.KeyValueRecord
import com.amazon.milan.graph.{DependencyGraph, FlowGraph, StreamCollection}
import com.amazon.milan.program.ExternalStream
import com.amazon.milan.typeutil.{TypeDescriptor, types}
import org.junit.Test

import java.io.ByteArrayOutputStream


@Test
class TestAwsServerlessGeneratorPlugin {
  @Test
  def test_AwsServerlessGeneratorPlugin_GenerateDataSink_WithDynamoDbTableSink_ProducesCode(): Unit = {
    val plugin = new AwsServerlessGeneratorPlugin(TypeLifter.createDefault())

    val streams = new StreamCollection(List.empty)
    val config = new ApplicationConfiguration()
    val application = new ApplicationInstance(new Application(streams), config)
    val dependencyGraph = DependencyGraph.build(streams.getDereferencedStreams)
    val flowGraph = FlowGraph.build(streams.getDereferencedStreams)
    val outputs = new GeneratorOutputs(plugin.typeLifter.typeEmitter)
    val plugins = EventHandlerGeneratorPlugin.EMPTY

    val context =
      GeneratorContext(
        application,
        outputs,
        ExpressionContext(Map.empty),
        dependencyGraph,
        flowGraph,
        plugins)
    val sink = new DynamoDbTableSink[KeyValueRecord]("outputTableSink", "table")

    val streamInfo =
      StreamInfo(
        ExternalStream("streamId", "streamId", TypeDescriptor.of[KeyValueRecord].toDataStream),
        types.EmptyTuple,
        types.EmptyTuple
      )

    plugin.generateDataSink(context, streamInfo, sink)

    // We can't compile this code at runtime because we get an error that says
    // "illegal cyclic reference involving class AttributeValue".
    // So we're just here to make sure the code generation doesn't fail.
    val outputStream = new ByteArrayOutputStream()
    context.outputs.generate("TestClass", outputStream)
  }
}
