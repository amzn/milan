package com.amazon.milan.compiler.scala.event

import com.amazon.milan.application.ApplicationInstance
import com.amazon.milan.compiler.scala._
import com.amazon.milan.graph.{DependencyGraph, FlowGraph}
import com.amazon.milan.program.StreamExpression
import com.amazon.milan.typeutil.TypeDescriptor

import java.io.OutputStream
import java.util.UUID


/**
 * Describes a consumer of a stream.
 *
 * @param consumerIdentifier An identifier of the expression or sink that is consuming a stream.
 * @param inputIdentifier    An identifier that specifies which input of the expression is being referred to.
 *                           For example, a JoinExpression will use "left" and "right" to identify the different inputs.
 */
case class StreamConsumerInfo(consumerIdentifier: String, inputIdentifier: String)


/**
 * Contains the context for the code generation operation.
 *
 * @param application       The Milan application being generated.
 * @param outputs           The generator output collector.
 * @param expressionContext The context of the expression being generated.
 * @param dependencyGraph   A [[DependencyGraph]] of the Milan application.
 * @param flowGraph         A [[FlowGraph]] of the Milan application.
 * @param plugin            The plugin that provides generator extensions.
 */
case class GeneratorContext(application: ApplicationInstance,
                            outputs: GeneratorOutputs,
                            expressionContext: ExpressionContext,
                            dependencyGraph: DependencyGraph,
                            flowGraph: FlowGraph,
                            plugin: EventHandlerGeneratorPlugin) {
  def withStreamTerm(name: String, stream: StreamInfo): GeneratorContext = {
    GeneratorContext(
      this.application,
      this.outputs,
      this.expressionContext.withStreamTerm(name, stream),
      this.dependencyGraph,
      this.flowGraph,
      this.plugin)
  }
}


/**
 * Encapsulates the context for expression generation.
 *
 * @param streamTerms Named terms and their corresponding [[StreamInfo]] objects.
 *                    If the expression being generated is inside a map function where one or more arguments are
 *                    streams, these terms map the function arguments to the streams they refer to.
 */
case class ExpressionContext(streamTerms: Map[String, StreamInfo]) {
  def withStreamTerm(name: String, stream: StreamInfo): ExpressionContext = {
    ExpressionContext(streamTerms + (name -> stream))
  }
}


/**
 * Describes a consumer method for an external stream.
 *
 * @param streamName         The name of the external stream.
 * @param consumerMethodName The name of the consumer method.
 * @param recordType         The record type that the method consumes.
 */
case class ExternalStreamConsumer(streamId: String, streamName: String, consumerMethodName: MethodName, recordType: TypeDescriptor[_])


/**
 * Collects the outputs from code generation.
 */
class GeneratorOutputs(typeEmitter: TypeEmitter) {
  private var methods: List[String] = List.empty
  private var fields: List[String] = List.empty
  private var fieldNames: Set[String] = Set.empty
  private var collectorNames: Set[String] = Set.empty
  private var consumerIdentifiers: Set[String] = Set.empty

  private var externalStreamMethods: List[ExternalStreamConsumer] = List.empty

  private var generatedStreams: Map[String, StreamInfo] = Map.empty

  val loggerField: ValName = ValName("logger")

  val scalaGenerator = new ScalarFunctionGenerator(this.typeEmitter, new IdentityTreeTransformer)

  /**
   * Registers a generated external stream with the output collector.
   *
   * @param streamId             The ID of the stream.
   * @param streamName           The name of the stream.
   * @param streamConsumerMethod The method that consumes stream records.
   * @param recordType           The type of stream records.
   */
  def addExternalStream(streamId: String,
                        streamName: String,
                        streamConsumerMethod: MethodName,
                        recordType: TypeDescriptor[_]): Unit = {
    this.externalStreamMethods = this.externalStreamMethods :+
      ExternalStreamConsumer(streamId, streamName, streamConsumerMethod, recordType)
  }

  /**
   * Gets information about the methods generated to consume records for external streams.
   *
   * @return A list of [[ExternalStreamConsumer]] objects that describe the external stream event handler methods.
   */
  def getExternalStreams: Iterable[ExternalStreamConsumer] =
    this.externalStreamMethods

  /**
   * Registers a generated stream with the output collector.
   *
   * @param streamId The ID of the stream.
   * @param stream   A [[StreamInfo]] describing the generated stream.
   */
  def addGeneratedStream(streamId: String, stream: StreamInfo): Unit = {
    this.generatedStreams = this.generatedStreams + (streamId -> stream)
  }

  /**
   * Gets the [[StreamInfo]] corresponding to a stream ID.
   *
   * @param streamId A stream ID.
   * @return The [[StreamInfo]] for the stream, or None if that stream has not been registered with the output
   *         collector.
   */
  def getGeneratedStream(streamId: String): Option[StreamInfo] = {
    this.generatedStreams.get(streamId)
  }

  /**
   * Gets all generated streams.
   */
  def getGeneratedStreams: Iterable[StreamInfo] =
    this.generatedStreams.values

  /**
   * Gets the name of the collector method that distributes records for a stream.
   *
   * @param provider The stream providing the records.
   * @return The name of the method that takes the stream records and distributes them to operations that consume those
   *         records.
   */
  def getCollectorName(provider: StreamExpression): MethodName = {
    MethodName(toValidName(s"collect_${provider.nodeName}"))
  }

  /**
   * Gets the name of the consumer method for a stream consumer.
   *
   * @param consumerInfo A [[StreamConsumerInfo]] describing the consumer.
   * @return The name of the consumer method corresponding to the stream consumer.
   */
  def getConsumerName(consumerInfo: StreamConsumerInfo): MethodName = {
    this.getConsumerName(consumerInfo.consumerIdentifier, consumerInfo.inputIdentifier)
  }

  /**
   * Gets the name of the consumer method for a stream consumer.
   *
   * @param consumerIdentifier An identifier of an operation.
   * @param inputIdentifier    An identifier of an input of the operation.
   * @return The name of the consumer method corresponding to the stream consumer.
   */
  def getConsumerName(consumerIdentifier: String, inputIdentifier: String): MethodName = {
    MethodName(toValidIdentifier(s"consume_${consumerIdentifier}_$inputIdentifier"))
  }

  /**
   * Adds a method to the generated class.
   *
   * @param methodDefinition The full method definition, including any access modified.
   */
  def addMethod(methodDefinition: String): Unit = {
    this.methods = this.methods :+ methodDefinition
  }

  /**
   * Adds a collector method to the output.
   *
   * @param collectorName    The name of the method, used to ensure no duplicates are added.
   * @param methodDefinition The method definition.
   */
  def addCollectorMethod(collectorName: String, methodDefinition: String): Unit = {
    if (this.collectorNames.contains(collectorName)) {
      throw new IllegalArgumentException(s"Collector method $collectorName already exists.")
    }

    this.collectorNames = this.collectorNames + collectorName
    this.addMethod(methodDefinition)
  }

  /**
   * Gets whether a collector has already been added.
   *
   * @param collectorName The name of the collector
   * @return True if the collector has already been added, otherwise false.
   */
  def collectorExists(collectorName: String): Boolean =
    this.collectorNames.contains(collectorName)

  /**
   * Adds a field to the generated class.
   *
   * @param fieldDefinition The full field definition, including any access modifier.
   */
  def addField(fieldDefinition: String): Unit = {
    this.fields = this.fields :+ fieldDefinition
  }

  /**
   * Gets a unique, valid field name with the specified prefix.
   *
   * @param prefix The field prefix.
   * @return A field name.
   */
  def newFieldName(prefix: String): String = {
    val name = toValidIdentifier(prefix + UUID.randomUUID().toString.substring(0, 4))
    if (this.fieldNames.contains(name)) {
      this.newFieldName(prefix)
    }
    else {
      this.fieldNames = this.fieldNames + name
      name
    }
  }

  /**
   * Gets a unique consumer identifier with the specified prefix.
   */
  def newConsumerIdentifier(prefix: String): String = {
    val name = toValidIdentifier(prefix + UUID.randomUUID().toString.substring(0, 4))
    if (this.consumerIdentifiers.contains(name)) {
      this.newConsumerIdentifier(prefix)
    }
    else {
      this.consumerIdentifiers = this.consumerIdentifiers + name
      name
    }
  }

  /**
   * Writes the definition of the generated class to an output stream.
   *
   * @param className    The name of the class to generate.
   * @param outputStream The output stream where the class definition will be written.
   */
  def generate(className: String, outputStream: OutputStream): Unit = {
    outputStream.writeUtf8(s"class $className extends com.amazon.milan.compiler.scala.event.RecordConsumer {\n")

    this.fields.foreach(field => {
      outputStream.writeUtf8(field.indent(1))
      outputStream.writeUtf8("\n\n")
    })
    outputStream.writeUtf8("\n\n")

    val consumeParts =
      this.externalStreamMethods
        .map(method =>
          s"""case "${method.streamName}" => ${method.consumerMethodName}(value.asInstanceOf[${method.recordType.toTerm}])"""
        )
        .mkString("\n")

    val consumeMethod =
      s"""override def consume(inputName: String, value: Any): Unit = {
         |  inputName match {
         |    ${consumeParts.indentTail(2)}
         |    case _ => throw new IllegalArgumentException(s"There is no input named '$$inputName'.")
         |  }
         |}
         |""".codeStrip

    outputStream.writeUtf8(consumeMethod.indent(1))
    outputStream.writeUtf8("\n\n")

    this.methods.foreach(method => {
      outputStream.writeUtf8(method.indent(1))
      outputStream.writeUtf8("\n\n")
    })

    outputStream.writeUtf8("}\n")
  }
}
