package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.sinks.{DynamoDbTableSink, SqsDataSink}
import com.amazon.milan.application.{DataSink, StateStore}
import com.amazon.milan.aws.serverless.application.DynamoDbStateStore
import com.amazon.milan.aws.serverless.runtime.SqsRecordWriter
import com.amazon.milan.aws.serverless.{DynamoDbObjectStore, ObjectStoreKeyedStateInterface}
import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.scala.event._
import com.amazon.milan.program.{FunctionDef, SelectTerm, StreamExpression, ValueDef}
import com.amazon.milan.tools.InstanceParameters
import com.amazon.milan.typeutil.{TypeDescriptor, types}


/**
 * A plugin for the event handler generator to generate state stores for use by AWS serverless functions.
 */
class AwsServerlessGeneratorPlugin(val typeLifter: TypeLifter, compilerParameters: InstanceParameters)
  extends EventHandlerGeneratorPlugin
    with ConsumerGenerator {

  import typeLifter._

  private val dynamoDbGenerator = new DynamoDbItemGenerator(this.typeLifter)

  override def generateKeyedStateInterface(context: GeneratorContext,
                                           streamExpr: StreamExpression,
                                           stateIdentifier: String,
                                           keyType: TypeDescriptor[_],
                                           stateType: TypeDescriptor[_],
                                           stateConfig: StateStore): Option[CodeBlock] = {
    stateConfig match {
      case ddb: DynamoDbStateStore =>
        Some(this.generateDynamoDbKeyedStateInterface(context, streamExpr.nodeId, stateIdentifier, ddb, keyType, stateType))

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

      case sqs: SqsDataSink[_] =>
        Some(this.generateSqsDataSink(context, stream, sqs))

      case _ =>
        None
    }
  }

  override def getDefaultStateStore(streamExpr: StreamExpression, stateIdentifier: String): Option[StateStore] = {
    val idFunc = FunctionDef(List(ValueDef("a", streamExpr.recordType)), SelectTerm("a"))
    Some(new DynamoDbStateStore(idFunc, idFunc))
  }

  private def generateDynamoDbKeyedStateInterface(context: GeneratorContext,
                                                  streamId: String,
                                                  stateId: String,
                                                  stateConfig: DynamoDbStateStore,
                                                  keyType: TypeDescriptor[_],
                                                  stateType: TypeDescriptor[_]): CodeBlock = {
    // The table name can be set via the class properties.
    val tableNameDefaultValue = stateConfig.tableName.map(name => qc"$name")
    val tableNameValue = context.outputs.addStateStoreProperty(
      streamId, stateId, "TableName", types.String, tableNameDefaultValue
    )

    qc"""${nameOf[DynamoDbObjectStore[Any, Any]]}.createKeyedStateInterface[${keyType.toTerm}, ${stateType.toTerm}](
        |  $tableNameValue,
        |  $keyType,
        |  $stateType)
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

  private def generateSqsDataSink(context: GeneratorContext,
                                  stream: StreamInfo,
                                  sink: SqsDataSink[_]): StreamConsumerInfo = {
    val consumerId = context.outputs.newConsumerIdentifier(s"${stream.streamId}_sink_")
    val consumerInfo = StreamConsumerInfo(consumerId, "input")

    val recordType = stream.recordType

    // The queue URL can be passed in when creating an instance of the application.
    val queueUrlDefaultValue = sink.queueUrl.map(url => qc"$url")
    val queueUrlValue = context.outputs.addSinkProperty(
      stream.streamId, sink.sinkId, "QueueUrl", types.String, queueUrlDefaultValue
    )

    val writerFieldName = ValName(context.outputs.newFieldName(s"recordWriter_${stream.streamId}_"))
    val writerField = qc"""private val $writerFieldName = ${nameOf[SqsRecordWriter[Any]]}.open[${recordType.toTerm}]($queueUrlValue)"""

    context.outputs.addField(writerField.value)

    val recordArg = qv"record"

    val methodBody =
      qc"""
          |$writerFieldName.writeRecord($recordArg.value)
          |"""

    this.generateConsumer(context.outputs, stream, consumerInfo, recordArg, methodBody)

    consumerInfo
  }
}
