package com.amazon.milan.aws.serverless.compiler

import com.amazon.milan.application.StateStore
import com.amazon.milan.aws.serverless.{DynamoDbObjectStore, ObjectStoreKeyedStateInterface}
import com.amazon.milan.aws.serverless.application.DynamoDbStateStore
import com.amazon.milan.compiler.scala.event.{EventHandlerGeneratorPlugin, GeneratorContext, GeneratorOutputs}
import com.amazon.milan.compiler.scala._
import com.amazon.milan.program.StreamExpression
import com.amazon.milan.typeutil.TypeDescriptor


class AwsServerlessGeneratorPlugin(typeLifter: TypeLifter) extends EventHandlerGeneratorPlugin {

  import typeLifter._

  override def generateKeyedStateStore(context: GeneratorContext,
                                       streamExpr: StreamExpression,
                                       keyType: TypeDescriptor[_],
                                       stateType: TypeDescriptor[_],
                                       stateConfig: StateStore): Option[CodeBlock] = {
    stateConfig match {
      case ddb: DynamoDbStateStore =>
        Some(this.generateDynamoDbKeyedStateStore(context.outputs, ddb, keyType, stateType))
    }
  }

  private def generateDynamoDbKeyedStateStore(outputs: GeneratorOutputs,
                                              stateConfig: DynamoDbStateStore,
                                              keyType: TypeDescriptor[_],
                                              stateType: TypeDescriptor[_]): CodeBlock = {
    qc"""${nameOf[DynamoDbObjectStore[Any, Any]]}.open[${keyType.toTerm}, ${stateType.toTerm}](
        |   ${stateConfig.tableName},
        |   $keyType,
        |   $stateType)
        |"""
  }
}
