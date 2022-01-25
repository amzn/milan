package com.amazon.milan.compiler.scala.event

import com.amazon.milan.application.StateStore
import com.amazon.milan.application.state.DefaultStateStore
import com.amazon.milan.compiler.scala._
import com.amazon.milan.program.StreamExpression
import com.amazon.milan.typeutil.TypeDescriptor


trait StateInterfaceGenerator {
  val typeLifter: TypeLifter

  import typeLifter._

  /**
   * Generates a code block that returns a [[KeyedStateInterface]] for use by an operator.
   *
   * @param context         The generator context.
   * @param expr            A [[StreamExpression]] whose state interface is being generated.
   * @param stateIdentifier Identifies which of the operation's state stores is being generated.
   * @param keyType         A [[TypeDescriptor]] describing the type of keys for the objects that will be stored.
   * @param stateType       A [[TypeDescriptor]] describing the type of object that will be stored.
   * @return A [[CodeBlock]] that returns the keyed state store interface instance.
   */
  def generateKeyedStateInterface(context: GeneratorContext,
                                  expr: StreamExpression,
                                  stateIdentifier: String,
                                  keyType: TypeDescriptor[_],
                                  stateType: TypeDescriptor[_]): CodeBlock = {
    val stateConfig = context.application.config.getStateStore(expr.nodeId, stateIdentifier)

    // Try the plugin first, then fall back to the known state store types.
    context.plugin.generateKeyedStateStore(context, expr, keyType, stateType, stateConfig) match {
      case Some(code) =>
        code

      case None =>
        this.generateKeyedStateInterface(context.outputs, stateConfig, keyType, stateType)
    }
  }

  /**
   * Generates a code block that returns a [[KeyedStateInterface]] for use by an operator.
   *
   * @param outputs     The generator outputs collector.
   * @param stateConfig A [[StateStore]] containing the state store configuration.
   * @param keyType     A [[TypeDescriptor]] describing the type of keys for the objects that will be stored.
   * @param stateType   A [[TypeDescriptor]] describing the type of object that will be stored.
   * @return A [[CodeBlock]] that returns the keyed state store interface instance.
   */
  def generateKeyedStateInterface(outputs: GeneratorOutputs,
                                  stateConfig: StateStore,
                                  keyType: TypeDescriptor[_],
                                  stateType: TypeDescriptor[_]): CodeBlock = {
    stateConfig match {
      case _: DefaultStateStore =>
        qc"new ${nameOf[MemoryKeyedState[Any, Any]]}[${keyType.toTerm}, ${stateType.toTerm}]"
    }
  }
}
