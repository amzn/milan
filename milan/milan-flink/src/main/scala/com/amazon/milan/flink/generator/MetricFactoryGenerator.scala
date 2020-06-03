package com.amazon.milan.flink.generator

import com.amazon.milan.flink.metrics.OperatorMetricFactory
import com.amazon.milan.program.StreamExpression


trait MetricFactoryGenerator {
  val typeLifter: TypeLifter

  import typeLifter._

  def generateMetricFactory(context: GeneratorContext, streamExpr: StreamExpression): CodeBlock = {
    qc"new ${nameOf[OperatorMetricFactory]}(${context.appConfig.getMetricPrefix}, ${streamExpr.nodeName})"
  }
}
