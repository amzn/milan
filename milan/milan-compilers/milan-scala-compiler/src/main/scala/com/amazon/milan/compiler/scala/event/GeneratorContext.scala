package com.amazon.milan.compiler.scala.event

import com.amazon.milan.application.{ApplicationInstance, StateStore}
import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.scala.event.ClassPropertySourceType.ClassPropertySourceType
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


object ClassPropertySourceType extends Enumeration {
  type ClassPropertySourceType = Value

  val StreamSource, StreamSink, StateStore = Value
}


/**
 * Describes the source of a property value.
 *
 * @param sourceType     The type of the property source.
 * @param sourceId       The ID of the source. For stream sources this will be the stream ID.
 *                       For sinks, it will be the "streamId:sinkId".
 * @param sourceProperty The property of the source that maps to this property value.
 *                       The values this can take depend on the specific type of the source or sink.
 */
case class ClassPropertySource(sourceType: ClassPropertySourceType, sourceId: String, sourceProperty: String)

/**
 * Describes a field of the Properties class required by the generated code.
 * The generated class will have a Properties class that allows callers to specify many of the properties of the
 * data sinks and sources.
 *
 * @param propertyName The name of the property, which must be a valid field name.
 * @param propertyType The type of the property.
 * @param defaultValue The default value which is used when the property is not supplied.
 *                     None means no default value and the property must be supplied when the generated code is invoked.
 */
case class ClassProperty(propertyName: String,
                         propertyType: TypeDescriptor[_],
                         defaultValue: Option[CodeBlock],
                         propertySource: ClassPropertySource)


/**
 * Describes a state store that is used by the compiled application.
 *
 * @param streamId   The ID of the stream the state store is used by.
 * @param stateId    The ID of the state, which is used to distinguish between different state stores for a single
 *                   stream.
 * @param stateStore The state store used.
 */
case class GeneratedStateStore(streamId: String,
                               stateId: String,
                               stateStore: StateStore)


/**
 * Collects the outputs from code generation.
 */
class GeneratorOutputs(typeEmitter: TypeEmitter) {
  private var methods: List[String] = List.empty
  private var fields: List[String] = List.empty
  private var fieldNames: Set[String] = Set.empty
  private var collectorNames: Set[String] = Set.empty
  private var consumerIdentifiers: Set[String] = Set.empty

  /**
   * Properties that generated code requires to be set.
   * The generated code assumes these properties are fields in the properties field of the generated class.
   */
  private var properties: List[ClassProperty] = List.empty

  private var externalStreamMethods: List[ExternalStreamConsumer] = List.empty

  private var generatedStreams: Map[String, StreamInfo] = Map.empty

  private var generatedStateStores: List[GeneratedStateStore] = List.empty

  val loggerField: ValName = ValName("logger")

  val propertiesField: ValName = ValName("props")

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
   * Adds generated state store information to the output.
   *
   * @param store The generated store.
   */
  def addGeneratedStateStore(store: GeneratedStateStore): Unit = {
    this.generatedStateStores = store +: this.generatedStateStores
  }

  /**
   * Gets information about the state stores that have been generated.
   */
  def getGeneratedStateStores: List[GeneratedStateStore] =
    this.generatedStateStores

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
   * Add a property that is required by the generated code.
   *
   * @param propertyName The name of the property, which must be a valid field name.
   * @param propertyType The type of the property.
   */
  def addProperty(propertyName: String,
                  propertyType: TypeDescriptor[_],
                  defaultValue: Option[CodeBlock],
                  propertySource: ClassPropertySource): Unit = {
    if (!this.properties.exists(_.propertyName == propertyName)) {
      this.properties = ClassProperty(propertyName, propertyType, defaultValue, propertySource) +: this.properties
    }
  }

  /**
   * Gets the class properties.
   * These are fields in the Properties class that is passed to the constructor of the generated class.
   *
   * @return
   */
  def getProperties: List[ClassProperty] = this.properties

  /**
   * Adds a required property that is a property of a data sink, returning a [[CodeBlock]] that resolves to the
   * property value at runtime.
   *
   * @param streamId     The ID of the stream the sink is attached to.
   * @param sinkId       The ID of the sink.
   * @param propertyName The name of the sink property.
   * @param propertyType The type of the property.
   * @param defaultValue The default value of the property, if there is one.
   * @return A [[CodeBlock]] that resolves to the property value at runtime.
   */
  def addSinkProperty(streamId: String,
                      sinkId: String,
                      propertyName: String,
                      propertyType: TypeDescriptor[_],
                      defaultValue: Option[CodeBlock]): CodeBlock = {
    val fieldName = toValidIdentifier(s"${streamId}_sink_${sinkId}_$propertyName")
    val source = ClassPropertySource(ClassPropertySourceType.StreamSink, s"$streamId:$sinkId", propertyName)
    this.addProperty(fieldName, propertyType, defaultValue, source)
    CodeBlock(s"${this.propertiesField}.$fieldName")
  }

  /**
   * Adds a class property used by a state store.
   *
   * @param streamId     The ID of the stream the state store is being used for.
   * @param stateId      The ID of the state. Some streams use multiple state stores.
   * @param propertyName The name of the property of the state store.
   * @param propertyType The type of the property.
   * @param defaultValue The default value of the property, if there is one.
   * @return A [[CodeBlock]] that resolves to the property value at runtime.
   */
  def addStateStoreProperty(streamId: String,
                            stateId: String,
                            propertyName: String,
                            propertyType: TypeDescriptor[_],
                            defaultValue: Option[CodeBlock]): CodeBlock = {
    val fieldName = toValidIdentifier(s"${streamId}_${stateId}_$propertyName")
    val source = ClassPropertySource(ClassPropertySourceType.StateStore, s"$streamId:$stateId", propertyName)
    this.addProperty(fieldName, propertyType, defaultValue, source)
    CodeBlock(s"${this.propertiesField}.$fieldName")
  }

  /**
   * Writes the definition of the generated class to an output stream.
   *
   * @param className    The name of the class to generate.
   * @param outputStream The output stream where the class definition will be written.
   */
  def generate(className: String, outputStream: OutputStream): GeneratorOutputInfo = {
    val propertiesClassDef = this.generatePropertiesClass()

    val propertiesClassName = s"$className.Properties"
    val defaultConstructor = this.generateOptionalDefaultConstructor(propertiesClassName)

    val classDefStart =
      s"""object $className {
         |  ${propertiesClassDef.indentTail(1)}
         |}
         |
         |class $className(protected val ${this.propertiesField}: $propertiesClassName)
         |  extends com.amazon.milan.compiler.scala.event.RecordConsumer {
         |
         |  ${defaultConstructor.indentTail(1)}
         |
         |""".stripMargin

    outputStream.writeUtf8(classDefStart)


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

    GeneratorOutputInfo(
      className,
      s"$className.Properties",
      this.properties,
      this.getGeneratedStreams.toList,
      this.getExternalStreams.toList,
      this.getGeneratedStateStores
    )
  }

  private def generateOptionalDefaultConstructor(propertiesClass: String): String = {
    val allPropertiesHaveDefaultValues = this.properties.forall(_.defaultValue.isDefined)

    // If all properties have default values then we can generate a default constructor for the event handler.
    if (!allPropertiesHaveDefaultValues) {
      ""
    }
    else {
      s"""def this() {
         |  this(new $propertiesClass)
         |}
         |""".stripMargin
    }
  }

  private def generatePropertiesClass(): String = {
    val propertyVals = this.properties.map(
      prop => {
        prop.defaultValue match {
          case Some(value) =>
            s"val ${prop.propertyName}: ${prop.propertyType.toTerm} = $value"

          case None =>
            s"val ${prop.propertyName}: ${prop.propertyType.toTerm}"
        }
      }
    )

    val propertyList = propertyVals.mkString(",\n                 ")

    s"class Properties($propertyList)"
  }
}
