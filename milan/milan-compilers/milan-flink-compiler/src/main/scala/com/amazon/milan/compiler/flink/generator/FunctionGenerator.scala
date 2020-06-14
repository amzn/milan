package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.compiler.scala.CodeBlock
import com.amazon.milan.compiler.flink.types.ArrayRecord
import com.amazon.milan.compiler.flink.typeutil._
import com.amazon.milan.program.FunctionDef
import com.amazon.milan.typeutil.TypeDescriptor


trait FunctionGenerator {
  val typeLifter: FlinkTypeLifter

  import typeLifter._

  def getFunctionDefinition(output: GeneratorOutputs,
                            functionName: String,
                            functionDef: FunctionDef,
                            outputType: TypeDescriptor[_]): CodeBlock = {
    val finalFunctionDef = this.replaceStreamArgumentsWithIterable(functionDef)

    if (outputType.isTupleRecord) {
      val parts = output.scalaGenerator.getScalaFunctionParts(finalFunctionDef)

      qc"""def ${code(functionName)}${parts.arguments}: ${nameOf[ArrayRecord]} = {
          |  val t = {
          |    ${parts.body.indentTail(2)}
          |  }
          |
          |  new ${nameOf[ArrayRecord]}(Array(${code(List.tabulate(finalFunctionDef.tpe.fields.length)(i => s"t._${i + 1}").mkString(", "))}))
          |}
          |"""
    }
    else {
      CodeBlock(output.scalaGenerator.getScalaFunctionDef(functionName, finalFunctionDef))
    }
  }

  private def replaceStreamArgumentsWithIterable(functionDef: FunctionDef): FunctionDef = {
    def replaceStreamWithIterable(argType: TypeDescriptor[_]): TypeDescriptor[_] = {
      if (argType.isStream) {
        argType.asStream.recordType.toIterable
      }
      else {
        argType
      }
    }

    val newArgTypes = functionDef.arguments.map(arg => replaceStreamWithIterable(arg.tpe))
    functionDef.withArgumentTypes(newArgTypes)
  }
}
