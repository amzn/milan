package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.sinks.DynamoDbTableSink
import com.amazon.milan.application.{DataSink, StateStore}
import com.amazon.milan.aws.serverless.DynamoDbObjectStore
import com.amazon.milan.aws.serverless.application.DynamoDbStateStore
import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.scala.event.{ConsumerGenerator, EventHandlerGeneratorPlugin, GeneratorContext, StreamConsumerInfo, StreamInfo}
import com.amazon.milan.program.StreamExpression
import com.amazon.milan.typeutil.TypeDescriptor


/**
 * A plugin for the event handler generator to generate state stores for use by AWS serverless functions.
 */
class AwsServerlessGeneratorPlugin(val typeLifter: TypeLifter)
  extends EventHandlerGeneratorPlugin
    with ConsumerGenerator {

  import typeLifter._

  private val dynamoDbGenerator = new DynamoDbItemGenerator(this.typeLifter)

  override def generateKeyedStateStore(context: GeneratorContext,
                                       streamExpr: StreamExpression,
                                       keyType: TypeDescriptor[_],
                                       stateType: TypeDescriptor[_],
                                       stateConfig: StateStore): Option[CodeBlock] = {
    stateConfig match {
      case ddb: DynamoDbStateStore =>
        Some(this.generateDynamoDbKeyedStateStore(ddb, keyType, stateType))

      case _ =>
        None
    }
  }

  override def generateDataSink(context: GeneratorContext,
                                stream: StreamInfo,
                                sink: DataSink[_]): Option[StreamConsumerInfo] = {
    sink match {
      case ddb: DynamoDbTableSink[_] =>
        Some(this.generateDynamoDbTableSink(context, stream, ddb))

      case _ =>
        None
    }
  }

  private def generateDynamoDbKeyedStateStore(stateConfig: DynamoDbStateStore,
                                              keyType: TypeDescriptor[_],
                                              stateType: TypeDescriptor[_]): CodeBlock = {
    qc"""${nameOf[DynamoDbObjectStore[Any, Any]]}.open[${keyType.toTerm}, ${stateType.toTerm}](
        |   ${stateConfig.tableName},
        |   $keyType,
        |   $stateType)
        |"""
  }

  private def generateDynamoDbTableSink(context: GeneratorContext,
                                        stream: StreamInfo,
                                        sink: DynamoDbTableSink[_]): StreamConsumerInfo = {
    val consumerId = context.outputs.newConsumerIdentifier(s"${stream.streamId}_sink_")
    val consumerInfo = StreamConsumerInfo(consumerId, "input")

    val writerFieldName = ValName(this.dynamoDbGenerator.generateDynamoDbRecordWriterField(context, stream, sink))

    val recordArg = ValName("record")

    val methodBody =
      qc"""
          |$writerFieldName.writeRecord($recordArg.value)
          |"""

    this.generateConsumer(context.outputs, stream, consumerInfo, recordArg, methodBody)

    consumerInfo
  }

}
