package com.amazon.milan.compiler.scala.event

import com.amazon.milan.application.StateStore
import com.amazon.milan.application.state.{DefaultStateStore, MemoryStateStore}
import com.amazon.milan.compiler.scala._
import com.amazon.milan.program.StreamExpression
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


trait StateInterfaceGenerator {
  val typeLifter: TypeLifter

  private val logger = Logger(LoggerFactory.getLogger(this.getClass))

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

    val finalStateConfig = stateConfig match {
      case _: DefaultStateStore =>
        this.getDefaultStateStore(context, expr, stateIdentifier)

      case _ =>
        stateConfig
    }

    this.logger.info(s"State interface for stream '${expr.nodeId}' state '$stateIdentifier' = ${stateConfig.getClass.getName}")

    context.outputs.addGeneratedStateStore(GeneratedStateStore(expr.nodeId, stateIdentifier, finalStateConfig))

    val stateInterface =
    // Try the plugin first, then fall back to the known state store types.
      context.plugin.generateKeyedStateInterface(context, expr, stateIdentifier, keyType, stateType, finalStateConfig) match {
        case Some(code) =>
          code

        case None =>
          this.generateKeyedStateInterface(context.outputs, finalStateConfig, keyType, stateType)
      }

    stateInterface
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
      case _: MemoryStateStore =>
        qc"new ${nameOf[MemoryKeyedState[Any, Any]]}[${keyType.toTerm}, ${stateType.toTerm}]"
    }
  }

  private def getDefaultStateStore(context: GeneratorContext,
                                   stream: StreamExpression,
                                   stateId: String): StateStore = {
    context.plugin.getDefaultStateStore(stream, stateId) match {
      case Some(store) =>
        store

      case None =>
        new MemoryStateStore
    }
  }
}
