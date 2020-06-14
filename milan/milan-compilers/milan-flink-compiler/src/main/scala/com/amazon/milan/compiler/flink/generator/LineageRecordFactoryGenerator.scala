package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.compiler.scala.CodeBlock
import com.amazon.milan.SemanticVersion
import com.amazon.milan.compiler.flink.internal.{ComponentJoinLineageRecordFactory, ComponentLineageRecordFactory}
import com.amazon.milan.program.{StreamExpression, StreamMap}


trait LineageRecordFactoryGenerator {
  val typeLifter: FlinkTypeLifter

  import typeLifter._

  def generateJoinLineageRecordFactory(context: GeneratorContext,
                                       mapExpr: StreamMap,
                                       leftInputStream: GeneratedStream,
                                       rightInputStream: GeneratedStream): CodeBlock = {
    // TODO: Use the real component version.
    qc"new ${nameOf[ComponentJoinLineageRecordFactory]}(${leftInputStream.streamId}, ${rightInputStream.streamId}, ${context.applicationInstanceId}, ${mapExpr.nodeId}, ${mapExpr.nodeId}, ${SemanticVersion.ZERO})"
  }

  def generateLineageRecordFactory(context: GeneratorContext,
                                   streamExpr: StreamExpression,
                                   inputStream: GeneratedStream): CodeBlock = {
    // TODO: Use the real component version.
    qc"new ${nameOf[ComponentLineageRecordFactory]}(${inputStream.streamId}, ${context.applicationInstanceId}, ${streamExpr.nodeId}, ${streamExpr.nodeId}, ${SemanticVersion.ZERO})"
  }
}
