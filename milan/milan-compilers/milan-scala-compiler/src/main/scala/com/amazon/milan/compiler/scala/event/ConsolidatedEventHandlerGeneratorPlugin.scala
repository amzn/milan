package com.amazon.milan.compiler.scala.event

import com.amazon.milan.application.{DataSink, StateStore}
import com.amazon.milan.compiler.scala.CodeBlock
import com.amazon.milan.program.StreamExpression
import com.amazon.milan.typeutil.TypeDescriptor


class ConsolidatedEventHandlerGeneratorPlugin(plugins: List[EventHandlerGeneratorPlugin])
  extends EventHandlerGeneratorPlugin {

  override def describe(): String = {
    plugins.map(_.describe()).mkString("[", ", ", "]")
  }

  override def generateDataSink(context: GeneratorContext,
                                stream: StreamInfo,
                                sink: DataSink[_]): Option[StreamConsumerInfo] = {
    this.plugins.map(_.generateDataSink(context, stream, sink)).find(_.nonEmpty) match {
      case Some(value) => value
      case None => None
    }
  }

  override def generateKeyedStateInterface(context: GeneratorContext,
                                           streamExpr: StreamExpression,
                                           stateIdentifier: String,
                                           keyType: TypeDescriptor[_],
                                           stateType: TypeDescriptor[_],
                                           stateConfig: StateStore): Option[CodeBlock] = {
    this.plugins.map(_.generateKeyedStateInterface(context, streamExpr, stateIdentifier, keyType, stateType, stateConfig))
      .filter(_.isDefined)
      .map(_.get)
      .headOption
  }

  override def getDefaultStateStore(streamExpr: StreamExpression, stateIdentifier: String): Option[StateStore] = {
    this.plugins.map(_.getDefaultStateStore(streamExpr, stateIdentifier))
      .filter(_.isDefined)
      .map(_.get)
      .headOption
  }
}
