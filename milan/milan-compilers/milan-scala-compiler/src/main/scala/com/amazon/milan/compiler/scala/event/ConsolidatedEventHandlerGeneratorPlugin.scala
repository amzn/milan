package com.amazon.milan.compiler.scala.event

import java.util.Properties

import com.amazon.milan.application.{DataSink, StateStore}
import com.amazon.milan.compiler.scala.CodeBlock
import com.amazon.milan.program.StreamExpression
import com.amazon.milan.typeutil.TypeDescriptor


class ConsolidatedEventHandlerGeneratorPlugin(plugins: List[EventHandlerGeneratorPlugin])
  extends EventHandlerGeneratorPlugin {

  override def generateDataSink(context: GeneratorContext,
                                stream: StreamInfo,
                                sink: DataSink[_]): Option[StreamConsumerInfo] = {
    this.plugins.map(_.generateDataSink(context, stream, sink)).find(_.nonEmpty) match {
      case Some(value) => value
      case None => None
    }
  }

  override def generateKeyedStateStore(context: GeneratorContext,
                                       streamExpr: StreamExpression,
                                       keyType: TypeDescriptor[_],
                                       stateType: TypeDescriptor[_],
                                       stateConfig: StateStore): Option[CodeBlock] = {
    this.plugins.map(_.generateKeyedStateStore(context, streamExpr, keyType, stateType, stateConfig)).find(_.nonEmpty) match {
      case Some(value) => value
      case None => None
    }
  }
}
