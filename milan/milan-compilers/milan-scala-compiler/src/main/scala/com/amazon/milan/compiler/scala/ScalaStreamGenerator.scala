package com.amazon.milan.compiler.scala

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import com.amazon.milan.graph.{DependencyGraph, StreamCollection}
import com.amazon.milan.program.{ExternalStream, StreamExpression, Tree, ValueDef}
import com.amazon.milan.typeutil.TypeDescriptor
import com.amazon.milan.{graph, lang}

import scala.annotation.tailrec
import scala.collection.mutable


object ScalaStreamGenerator {
  /**
   * Generates code for an anonymous function which has the specified streams as its inputs and output.
   *
   * @param inputStreams A list of streams that will form the input arguments to the generated function.
   * @param returnStream The stream that will be returned from the generated function.
   * @return
   */
  def generateAnonymousFunction(inputStreams: List[lang.Stream[_]],
                                returnStream: lang.Stream[_]): String = {
    val inputStreamExpressions = inputStreams.map(_.expr)
    this.generateAnonymousFunction(inputStreamExpressions, returnStream.expr)
  }

  /**
   * Generates code for an anonymous function which has the specified streams as its inputs and output.
   *
   * @param inputStreams A list of streams that will form the input arguments to the generated function.
   * @param returnStream The stream that will be returned from the generated function.
   * @return
   */
  def generateAnonymousFunction(inputStreams: List[StreamExpression],
                                returnStream: StreamExpression): String = {
    val outputStream = new ByteArrayOutputStream()
    this.generateAnonymousFunction(inputStreams, returnStream, outputStream)
    StandardCharsets.UTF_8.decode(ByteBuffer.wrap(outputStream.toByteArray)).toString
  }

  /**
   * Generates code for an anonymous function which has the specified streams as its inputs and output.
   *
   * @param inputStreams A list of streams that will form the input arguments to the generated function.
   * @param returnStream The stream that will be returned from the generated function.
   * @param output       An [[OutputStream]] where the generated function will be written.
   * @return
   */
  def generateAnonymousFunction(inputStreams: List[StreamExpression],
                                returnStream: StreamExpression,
                                output: OutputStream): Unit = {
    val bodyStream = new ByteArrayOutputStream()
    val outputs = this.generateFunctionBody(returnStream, bodyStream)

    val externalStreamVals = outputs.getExternalStreams

    val inputStreamArgs =
      inputStreams.map(stream =>
        externalStreamVals.get(stream.nodeId) match {
          case Some(value) => value
          case None => ValueDef(toValidName(s"notused_${stream.nodeName}"), stream.recordType)
        }
      )

    val argsDef = inputStreamArgs.map(arg => s"${arg.name}: Stream[${outputs.scalaGenerator.typeEmitter.getTypeFullName(arg.tpe)}]").mkString("(", ", ", ")")
    output.writeUtf8(argsDef)
    output.writeUtf8(" => {\n")
    output.write(bodyStream.toByteArray)
    output.writeUtf8("\n}")
  }

  def generateFunction(functionName: String,
                       streams: StreamCollection,
                       outputStreamId: String,
                       output: OutputStream): Unit = {
    val returnStream = streams.getDereferencedStream(outputStreamId)

    val bodyStream = new ByteArrayOutputStream()
    val outputs = this.generateFunctionBody(returnStream, bodyStream, indentLevel = 2)

    val externalStreamVals = outputs.getExternalStreams

    val graph = DependencyGraph.build(returnStream)
    val inputStreams = graph.rootNodes.map(_.expr)

    val inputStreamArgs =
      inputStreams.map(stream =>
        externalStreamVals.get(stream.nodeId) match {
          case Some(value) => value
          case None => ValueDef(s"notused_${toValidIdentifier(stream.nodeName)}", stream.recordType)
        }
      )

    val argsDef = inputStreamArgs.map(arg => s"${arg.name}: Stream[${outputs.scalaGenerator.typeEmitter.getTypeFullName(arg.tpe)}]").mkString("(", ", ", ")")

    val returnTypeDef = s"Stream[${returnStream.recordType.toTerm.value}]"

    output.writeUtf8(s"  def $functionName$argsDef: $returnTypeDef = {\n")
    output.write(bodyStream.toByteArray)
    output.writeUtf8("  }")
  }

  private def generateFunctionBody(returnStream: StreamExpression,
                                   outputStream: OutputStream,
                                   indentLevel: Int = 0): GeneratorOutputs = {
    val graph = DependencyGraph.build(returnStream)
    val outputs = this.generate(graph)
    outputs.writeMainBlocks(outputStream, indentLevel)

    val returnStreamVal = outputs.streamValNames(returnStream.nodeId)

    outputStream.writeUtf8(s"$returnStreamVal")

    outputs
  }

  private def generate(dependencyGraph: DependencyGraph): GeneratorOutputs = {
    // Do a topological sort so that we define streams in the correct order in the generated code.
    val sortedStreams = dependencyGraph.topologicalSort.filter(_.contextStream.isEmpty).map(_.expr)
    graph.typeCheckGraph(sortedStreams)

    // First we generate ValNames for every stream, so that we can reference them later when generating code.
    val valNames =
      sortedStreams
        .map(stream => stream.nodeId -> ValName(toValidName(s"stream_${stream.nodeName}")))
        .toMap

    val outputs = new GeneratorOutputs(valNames)

    // Generate any streams that are not ExternalStreams, because those are arguments to the
    // function we are generating.
    sortedStreams.foreach(stream => this.ensureStreamIsGenerated(outputs, stream))

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
        outputs.addExternalStream(externalStream.nodeId, streamValName, externalStream.recordType)
        streamValName

      case _ =>
        val streamDefinition = outputs.scalaGenerator.generateScala(stream)
        outputs.appendMain(s"val $streamValName = $streamDefinition")
        streamValName
    }
  }

  class GeneratorOutputs(val streamValNames: Map[String, ValName]) {
    val generatedStreams = new mutable.HashMap[String, ValName]()
    val generatedGroupedStreams = new mutable.HashMap[String, ValName]()
    val scalaGenerator = new StreamFunctionGenerator(new DefaultTypeEmitter, streamValNames)

    private var mainBlocks = List.empty[String]
    private var valNames = Set.empty[String]
    private var externalStreams = List.empty[(String, ValueDef)]

    /**
     * Appends a code block to the main function being generated.
     *
     * @param block A code block.
     */
    def appendMain(block: String): Unit = {
      this.mainBlocks = this.mainBlocks :+ block
    }

    def addExternalStream(streamId: String, argName: ValName, recordType: TypeDescriptor[_]): Unit = {
      this.externalStreams = this.externalStreams :+ (streamId, ValueDef(argName.value, recordType))
    }

    def getExternalStreams: Map[String, ValueDef] =
      this.externalStreams.toMap

    def newValName(prefix: String): ValName = ValName(this.newName(prefix))

    def writeMainBlocks(outputStream: OutputStream, indentLevel: Int = 0): Unit = {
      mainBlocks.foreach(block => {
        outputStream.writeUtf8(block.indent(indentLevel))
        outputStream.writeUtf8("\n")
      })
    }

    @tailrec
    private def newName(prefix: String): String = {
      val name = toValidIdentifier(prefix + UUID.randomUUID().toString.substring(0, 4))
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
