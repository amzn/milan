package com.amazon.milan.compiler.scala.event

import com.amazon.milan.application.{DataSink, StateStore}
import com.amazon.milan.compiler.scala.{CodeBlock, TypeLifter}
import com.amazon.milan.program.StreamExpression
import com.amazon.milan.typeutil.TypeDescriptor

import java.io.InputStream
import java.util.Properties
import scala.collection.JavaConverters._


/**
 * Interface for classes that provide plugins for the event handler generator.
 */
trait EventHandlerGeneratorPlugin {
  /**
   * Generates a method in the output collector that implements a data sink.
   *
   * @param context The generator context.
   * @param stream  A [[StreamInfo]] representing the stream being sent to the sink.
   * @param sink    The data sink to generate.
   * @return A [[StreamConsumerInfo]] describing the generated sink function, or None if this plugin does not generate
   *         the given sink type.
   */
  def generateDataSink(context: GeneratorContext,
                       stream: StreamInfo,
                       sink: DataSink[_]): Option[StreamConsumerInfo] = None

  /**
   * Gets code block that instantiates a keyed state store interface.
   *
   * @param context     The generator context.
   * @param streamExpr  The operation whose state store is being generated.
   * @param keyType     A [[TypeDescriptor]] describing the type of keys for the objects that will be stored.
   * @param stateType   A [[TypeDescriptor]] describing the type of object that will be stored.
   * @param stateConfig A [[StateStore]] containing the state store configuration.
   * @return A [[CodeBlock]] that instantiates the keyed state store interface instance, or None if this plugin does not
   *         generate the given state store type.
   */
  def generateKeyedStateStore(context: GeneratorContext,
                              streamExpr: StreamExpression,
                              keyType: TypeDescriptor[_],
                              stateType: TypeDescriptor[_],
                              stateConfig: StateStore): Option[CodeBlock] = None
}


class EmptyEventHandlerGeneratorPlugin extends EventHandlerGeneratorPlugin {
}


object EventHandlerGeneratorPlugin {
  /**
   * The prefix of properties that specify event handler generator plugins.
   */
  val PLUGIN_CLASS_PROPERTY_PREFIX = "generator.eventhandlerplugin."

  /**
   * An empty plugin.
   */
  val EMPTY: EventHandlerGeneratorPlugin = new EmptyEventHandlerGeneratorPlugin

  /**
   * Gets a [[ConsolidatedEventHandlerGeneratorPlugin]] that includes all [[EventHandlerGeneratorPlugin]] classes
   * that were found in milan.properties files in the classpath.
   */
  def loadAllPlugins(typeLifter: TypeLifter): ConsolidatedEventHandlerGeneratorPlugin = {
    val classLoader = getClass.getClassLoader
    val propertiesFiles = classLoader.getResources("milan.properties")

    val pluginClassNames =
      propertiesFiles.asScala
        .flatMap(url => {
          val properties = this.loadProperties(url.openStream())

          properties
            .propertyNames().asScala
            .map(_.toString)
            .filter(_.startsWith(PLUGIN_CLASS_PROPERTY_PREFIX))
            .map(properties.getProperty)
        })
        .toList

    val plugins =
      pluginClassNames
        .map(classLoader.loadClass)
        .map(_.getConstructor(classOf[TypeLifter]))
        .map(_.newInstance(typeLifter))
        .map(_.asInstanceOf[EventHandlerGeneratorPlugin])

    new ConsolidatedEventHandlerGeneratorPlugin(plugins)
  }

  /**
   * Loads a [[Properties]] object from an input stream.
   */
  private def loadProperties(inputStream: InputStream): Properties = {
    val properties = new Properties()
    properties.load(inputStream)
    properties
  }
}
