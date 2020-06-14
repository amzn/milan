package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.flink.runtime.AssignSequenceNumberProcessAllWindowFunction
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}


trait ProcessWindowFunctionGenerator {
  val typeLifter: FlinkTypeLifter

  import typeLifter._

  def generateAssignSequenceNumberProcessAllWindowFunction(outputs: GeneratorOutputs,
                                                           recordType: TypeDescriptor[_],
                                                           keyType: TypeDescriptor[_],
                                                           streamIdentifier: String): ClassName = {
    val className = outputs.newClassName(s"ProcessAllWindowFunction_${streamIdentifier}_AssignSequenceNumber_")

    val classDef =
      q"""class $className
         |  extends ${nameOf[AssignSequenceNumberProcessAllWindowFunction[Any, Product, Window]]}[${recordType.toFlinkTerm}, ${keyType.toTerm}, ${nameOf[GlobalWindow]}] {
         |}
         |""".strip

    outputs.addClassDef(classDef)

    className
  }
}
