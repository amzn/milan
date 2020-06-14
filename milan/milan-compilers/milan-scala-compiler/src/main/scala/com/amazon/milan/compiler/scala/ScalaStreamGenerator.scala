package com.amazon.milan.compiler.scala

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID

import com.amazon.milan.lang
import com.amazon.milan.lang.StreamGraph
import com.amazon.milan.program.{ExternalStream, StreamExpression, Tree, ValueDef}
import com.amazon.milan.typeutil.TypeDescriptor

import scala.annotation.tailrec
import scala.collection.mutable


object ScalaStreamGenerator {
  def generateAnonymousFunction(graph: StreamGraph,
                                returnStream: lang.Stream[_]): String = {
    val outputStream = new ByteArrayOutputStream()
    this.generateAnonymousFunction(graph, returnStream, outputStream)
    StandardCharsets.UTF_8.decode(ByteBuffer.wrap(outputStream.toByteArray)).toString
  }

  def generateAnonymousFunction(graph: StreamGraph,
                                returnStream: lang.Stream[_],
                                output: OutputStream): Unit = {
    val bodyStream = new ByteArrayOutputStream()
    val outputs = this.generateFunctionBody(graph, returnStream, bodyStream)

    val argsDef = outputs.getExternalStreams.map(arg => s"${arg.name}: Stream[${outputs.scalaGenerator.typeEmitter.getTypeFullName(arg.tpe)}]").mkString("(", ", ", ")")
    output.writeUtf8(argsDef)
    output.writeUtf8(" => {\n")
    output.write(bodyStream.toByteArray)
    output.writeUtf8("\n}")
  }

  private def generateFunctionBody(graph: StreamGraph,
                                   returnStream: lang.Stream[_],
                                   outputStream: OutputStream): GeneratorOutputs = {
    val outputs = this.generate(graph)
    outputs.writeMainBlocks(outputStream)

    val returnStreamVal = outputs.streamValNames(returnStream.streamId)

    outputStream.writeUtf8(s"$returnStreamVal")

    outputs
  }

  private def generate(graph: StreamGraph): GeneratorOutputs = {
    graph.typeCheckGraph()

    val streams = graph.getStreams.toList

    // First we generate ValNames for every stream, so that we can reference them later when generating code.
    val valNames =
      streams
        .map(stream => stream.nodeId -> ValName(GeneratorOutputs.cleanName(s"stream_${stream.nodeName}")))
        .toMap

    val outputs = new GeneratorOutputs(valNames)

    // Generate any streams that are not ExternalStreams, because those are arguments to the
    // function we are generating.
    streams.foreach(stream => this.ensureStreamIsGenerated(outputs, stream))

    outputs
  }

  private def ensureStreamIsGenerated(outputs: GeneratorOutputs,
                                      stream: StreamExpression): Unit = {
    this.getOrGenerateDataStream(outputs, stream)
  }

  private def getOrGenerateDataStream(outputs: GeneratorOutputs,
                                      expr: Tree): ValName = {
    expr match {
      case streamExpr: StreamExpression =>
        outputs.generatedStreams.getOrElseUpdate(streamExpr.nodeId, this.generateDataStream(outputs, streamExpr))

      case _ =>
        throw new ScalaGeneratorException(s"Unrecognized data stream expression '$expr'.")
    }
  }

  private def generateDataStream(outputs: GeneratorOutputs,
                                 stream: StreamExpression): ValName = {
    val streamValName = outputs.streamValNames(stream.nodeId)

    stream match {
      case externalStream: ExternalStream =>
        outputs.addExternalStream(streamValName, externalStream.recordType)
        streamValName

      case _ =>
        val streamDefinition = outputs.scalaGenerator.generateScala(stream)
        outputs.appendMain(s"val $streamValName = $streamDefinition")
        streamValName
    }
  }

  object GeneratorOutputs {
    def cleanName(name: String): String =
      name.replace('-', '_')
  }

  import GeneratorOutputs._

  class GeneratorOutputs(val streamValNames: Map[String, ValName]) {
    val generatedStreams = new mutable.HashMap[String, ValName]()
    val generatedGroupedStreams = new mutable.HashMap[String, ValName]()
    val scalaGenerator = new StreamFunctionGenerator(new DefaultTypeEmitter, streamValNames)

    private var mainBlocks = List.empty[String]
    private var valNames = Set.empty[String]
    private var externalStreams = List.empty[ValueDef]

    /**
     * Appends a code block to the main function being generated.
     *
     * @param block A code block.
     */
    def appendMain(block: String): Unit = {
      this.mainBlocks = this.mainBlocks :+ block
    }

    def addExternalStream(argName: ValName, recordType: TypeDescriptor[_]): Unit = {
      this.externalStreams = this.externalStreams :+ ValueDef(argName.value, recordType)
    }

    def getExternalStreams: TraversableOnce[ValueDef] =
      this.externalStreams

    def newValName(prefix: String): ValName = ValName(this.newName(prefix))

    def writeMainBlocks(outputStream: OutputStream): Unit = {
      mainBlocks.foreach(block => {
        outputStream.writeUtf8(block)
        outputStream.writeUtf8("\n")
      })
    }

    @tailrec
    private def newName(prefix: String): String = {
      val name = cleanName(prefix + UUID.randomUUID().toString.substring(0, 8))
      if (this.valNames.contains(name)) {
        newName(prefix)
      }
      else {
        this.valNames = this.valNames + name
        name
      }
    }
  }

}
