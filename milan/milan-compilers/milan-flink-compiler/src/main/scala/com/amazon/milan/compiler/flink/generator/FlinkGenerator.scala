package com.amazon.milan.compiler.flink.generator

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}

import com.amazon.milan.application.{Application, ApplicationConfiguration, ApplicationInstance}
import com.amazon.milan.compiler.flink.internal.FlinkTypeEmitter
import com.amazon.milan.lang.StreamGraph
import com.amazon.milan.program.{Cycle, StreamExpression}
import com.amazon.milan.{Id, SemanticVersion}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory


case class GeneratorConfig(preventGenericTypeInformation: Boolean = false)


object FlinkGenerator {
  val default = new FlinkGenerator(GeneratorConfig())
}


class FlinkGenerator(classLoader: ClassLoader, generatorConfig: GeneratorConfig) {

  private val generatorTypeLifter = new FlinkTypeLifter(new FlinkTypeEmitter, this.generatorConfig.preventGenericTypeInformation)

  private val logger = Logger(LoggerFactory.getLogger(getClass))

  def this(generatorConfig: GeneratorConfig) {
    this(getClass.getClassLoader, generatorConfig)
  }

  def generateScala(graph: StreamGraph,
                    appConfig: ApplicationConfiguration,
                    packageName: String,
                    className: String): String = {
    val application = new Application(Id.newId(), graph, SemanticVersion.ZERO)
    val instance = new ApplicationInstance(Id.newId(), application, appConfig)
    this.generateScala(instance, packageName, className)
  }

  def generateScala(instance: ApplicationInstance,
                    outputPath: Path,
                    packageName: String,
                    className: String): Unit = {
    val scalaCode = this.generateScala(instance, packageName, className)
    val contents = scalaCode.getBytes(StandardCharsets.UTF_8)
    Files.write(outputPath, contents, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  def generateScala(instance: ApplicationInstance,
                    packageName: String,
                    className: String): String = {
    val output = new ByteArrayOutputStream()
    this.generateScala(instance, output, packageName, className)

    output.flush()
    StandardCharsets.UTF_8.decode(ByteBuffer.wrap(output.toByteArray)).toString
  }

  def generateScala(instance: ApplicationInstance,
                    output: OutputStream,
                    packageName: String,
                    className: String): Unit = {
    val finalGraph = instance.application.graph.getDereferencedGraph
    finalGraph.typeCheckGraph()

    val outputs = new GeneratorOutputs(this.generatorTypeLifter)
    val context = GeneratorContext.createEmpty(instance.instanceDefinitionId, finalGraph, instance.config, outputs, this.generatorTypeLifter)

    // Ensure that every data stream is generated.
    finalGraph
      .getStreams
      .foreach(stream => this.ensureStreamIsGenerated(context, stream))

    // Close any cycles.
    finalGraph
      .getStreams
      .filter(_.isInstanceOf[Cycle])
      .map(_.asInstanceOf[Cycle])
      .foreach(context.closeCycle)

    // Add all sinks at the end.
    instance.config.dataSinks.foreach(sink => context.generateSink(sink))

    val generated = context.output.generateScala(packageName, className)
    output.write(generated.getBytes(StandardCharsets.UTF_8))
  }

  private def ensureStreamIsGenerated(context: GeneratorContext,
                                      stream: StreamExpression): Unit = {
    context.getOrGenerateDataStream(stream)
  }
}
