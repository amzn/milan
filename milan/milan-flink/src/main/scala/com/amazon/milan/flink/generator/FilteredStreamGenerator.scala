package com.amazon.milan.flink.generator

import com.amazon.milan.flink.internal.TreeArgumentSplitter
import com.amazon.milan.flink.runtime.FilterFunctionBase
import com.amazon.milan.program.{Filter, FunctionDef, Tree, ValueDef}
import com.amazon.milan.typeutil.TypeDescriptor


trait FilteredStreamGenerator {
  val typeLifter: TypeLifter

  import typeLifter._

  /**
   * Applies a filter function to a stream.
   *
   * @param output     The generator output collector.
   * @param input      Information about the input stream.
   * @param filterExpr The filter expression.
   * @return The name of the val to which the resulting filtered stream was assigned.
   */
  def applyFilter(output: GeneratorOutputs,
                  input: GeneratedDataStream,
                  filterExpr: Filter): GeneratedDataStream = {
    this.applyFilter(output, filterExpr.nodeName, input, filterExpr.predicate)
  }

  /**
   * Applies a filter function to a stream.
   *
   * @param output           The generator output collector.
   * @param streamIdentifier An identifier for the output stream.
   * @param input            Information about the input stream.
   * @param filterFunction   The filter function definition.
   * @return The name of the val to which the resulting filtered stream was assigned.
   */
  def applyFilter(output: GeneratorOutputs,
                  streamIdentifier: String,
                  input: GeneratedDataStream,
                  filterFunction: FunctionDef): GeneratedDataStream = {
    val filterFunctionClassName = this.generateFilterFunction(output, streamIdentifier, input.recordType, input.keyType, filterFunction)

    val outputStreamVal = output.newStreamValName(streamIdentifier)
    val filterFunctionVal = output.newValName(s"stream_${streamIdentifier}_filterFunction_")

    output.appendMain(
      q"""
         |val $filterFunctionVal = new $filterFunctionClassName()
         |val $outputStreamVal = ${input.streamVal}.filter($filterFunctionVal)
         |""".strip)

    input.withStreamVal(outputStreamVal)
  }

  /**
   * Applies a filter operation using the portion of an expression tree that references the specified argument.
   *
   * @param output           The generator output collector.
   * @param outputIdentifier An identifier for the output stream.
   * @param input            Information about the input stream.
   * @param streamArgName    The name of the argument in the predicate expression that refers to the input stream.
   * @param filterExpression An expression tree that may contain filter conditions for the input stream.
   * @return The name of the val to which the resulting stream is assigned.
   *         This may be the same as inputVal if the filter expression is empty.
   */
  def applyFilterPortion(output: GeneratorOutputs,
                         outputIdentifier: String,
                         input: GeneratedDataStream,
                         streamArgName: String,
                         filterExpression: Tree): GeneratedDataStream = {
    TreeArgumentSplitter.splitTree(filterExpression, streamArgName).extracted match {
      case Some(filter) =>
        val streamFilterFunc = FunctionDef(List(ValueDef(streamArgName, input.recordType)), filter)
        this.applyFilter(output, outputIdentifier, input, streamFilterFunc)

      case None =>
        input
    }
  }

  /**
   * Generates a Flink FilterFunction class.
   *
   * @param output           The generator output collector.
   * @param outputIdentifier An identifier for the output stream.
   * @param inputRecordType  The record type of the stream being filtered.
   * @param keyType          The key type of the stream being filtered.
   * @param filterFunction   The filter function definition.
   * @return The name of the generated class.
   */
  private def generateFilterFunction(output: GeneratorOutputs,
                                     outputIdentifier: String,
                                     inputRecordType: TypeDescriptor[_],
                                     keyType: TypeDescriptor[_],
                                     filterFunction: FunctionDef): ClassName = {
    val className = output.newClassName(s"FilterFunction_$outputIdentifier")

    val flinkRecordFilterFunction = filterFunction.withArgumentTypes(List(inputRecordType))
    val filterFunctionCode = output.scalaGenerator.getScalaFunctionDef("filterImpl", flinkRecordFilterFunction)

    val classDef =
      q"""
         |class $className
         |  extends ${nameOf[FilterFunctionBase[Any, Product]]}[${inputRecordType.toFlinkTerm}, ${keyType.toTerm}]() {
         |
         |  protected override ${code(filterFunctionCode.indentTail(1))}
         |}
         |""".strip

    output.addClassDef(classDef)

    className
  }
}
