package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.compiler.scala.CodeBlock
import com.amazon.milan.compiler.flink.metrics.OperatorMetricFactory
import com.amazon.milan.program.StreamExpression


trait MetricFactoryGenerator {
  val typeLifter: FlinkTypeLifter

  import typeLifter._

  def generateMetricFactory(context: GeneratorContext, streamExpr: StreamExpression): CodeBlock = {
    qc"new ${nameOf[OperatorMetricFactory]}(${context.appConfig.getMetricPrefix}, ${streamExpr.nodeName})"
  }
}
