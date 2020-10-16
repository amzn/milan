package com.amazon.milan.compiler.scala

import com.amazon.milan.program.{ExternalStream, FlatMap, GroupBy, InvalidProgramException, LeftJoin, Ref, StreamArgMax, StreamArgMin, StreamMap, SumBy, Tree}
import com.amazon.milan.typeutil.{DataStreamTypeDescriptor, GroupedStreamTypeDescriptor}


object StreamFunctionGenerator {
  val default = new StreamFunctionGenerator(new DefaultTypeEmitter, Map.empty)
}


/**
 * Generates Scala functions that implement Milan stream operations by operating on Scala Streams.
 *
 * This allows generating Scala code from Milan applications that can be directly executed when the input data is
 * available as Scala Streams. This generator has several significant limitations:
 * 1. It does not support Join operations. Milan's Join operations are time-dependent and the code generated is not
 * event-based.
 * 2. It only supports a single output stream, which will be the stream returned by the generated function.
 *
 * @param typeEmitter A [[TypeEmitter]] used for generating type names.
 * @param refStreams  A map of stream IDs to [[ValName]] objects that can be used to reference those streams.
 */
class StreamFunctionGenerator(val typeEmitter: TypeEmitter, refStreams: Map[String, ValName])
  extends ScalarFunctionGenerator(typeEmitter, new IdentityTreeTransformer) {

  /**
   * Gets the Scala code that implements a Milan expression.
   */
  def generateScala(expr: Tree): String = {
    val context = new StreamContextWrapper(None, this.refStreams)
    context.getScalaCode(expr)
  }

  override protected def processContext(context: ConversionContext): ConversionContext = {
    new StreamContextWrapper(Some(context), refStreams)
  }

  /**
   * A [[ConversionContext]] that handles Milan Stream operations.
   *
   * @param innerContext A [[ConversionContext]] that use for handling all other operation types.
   */
  private class StreamContextWrapper(innerContext: Option[ConversionContext],
                                     refStreams: Map[String, ValName]) extends ConversionContext {
    override def getScalaCode(tree: Any): String = {
      tree match {
        case ExternalStream(nodeId, _, _) =>
          this.refStreams(nodeId).value

        case flatMapExpr: FlatMap =>
          this.generateFlatMap(flatMapExpr)

        case GroupBy(source, expr) =>
          scala"$source.groupBy($expr)"

        case Ref(nodeId) =>
          this.refStreams(nodeId).value

        case mapExpr: StreamMap =>
          this.generateMap(mapExpr)

        case StreamArgMax(source, expr) =>
          scala"com.amazon.milan.compiler.scala.runtime.maxBy($source, $expr)"

        case StreamArgMin(source, expr) =>
          scala"com.amazon.milan.compiler.scala.runtime.minBy($source, $expr)"

        case SumBy(source, argExpr, outputExpr) =>
          scala"com.amazon.milan.compiler.scala.runtime.sumBy($source, $argExpr, $outputExpr)"

        case _ =>
          // If we have an inner context then delegate to that, otherwise delegate to the base class.
          this.innerContext match {
            case Some(context) =>
              context.getScalaCode(tree)

            case None =>
              super.getScalaCode(tree)
          }
      }
    }

    override def generateSelectTermAndContext(name: String): (String, ConversionContext) = {
      this.innerContext match {
        case Some(context) =>
          context.generateSelectTermAndContext(name)

        case None =>
          throw new InvalidProgramException(s"Referencing terms by name is not a valid operation in this context.")
      }
    }

    private def generateMap(mapExpr: StreamMap): String = {
      mapExpr.source.tpe match {
        case _: GroupedStreamTypeDescriptor =>
          // Map.map sends a tuple, so we need to unpack it in the function body.
          val func = mapExpr.expr
          val keyArg = func.arguments.head
          val groupArg = func.arguments(1)

          val functionBodyContext = createContextForArguments(List(keyArg, groupArg))
          val functionBody = functionBodyContext.getScalaCode(func.body)

          // We need to produce another Map as the output, so use the syntax for that.
          scala"${mapExpr.source}.map { case ($keyArg, $groupArg) => ${keyArg.name} -> { $functionBody } }"

        case _: DataStreamTypeDescriptor =>
          scala"${mapExpr.source}.map(${mapExpr.expr})"

        case _ =>
          throw new NotImplementedError()      }
    }

    private def generateFlatMap(flatMapExpr: FlatMap): String = {
      flatMapExpr.source.tpe match {
        case _: GroupedStreamTypeDescriptor =>
          // Map.flatMap sends a tuple, so we need to unpack it in the function body.
          val func = flatMapExpr.expr
          val keyArg = func.arguments.head
          val groupArg = func.arguments(1)

          val functionBodyContext = createContextForArguments(List(keyArg, groupArg))
          val functionBody = functionBodyContext.getScalaCode(func.body)

          scala"${flatMapExpr.source}.flatMap { case ($keyArg, $groupArg) => $functionBody }"

        case _ =>
          throw new NotImplementedError()
      }
    }
  }

}
