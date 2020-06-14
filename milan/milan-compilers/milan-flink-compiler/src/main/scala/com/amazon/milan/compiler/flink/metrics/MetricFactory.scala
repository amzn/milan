package com.amazon.milan.compiler.flink.metrics

import org.apache.flink.api.common.functions.RuntimeContext


object MetricFactory {
  val typeName: String = getClass.getTypeName.stripSuffix("$")
}


trait MetricFactory extends Serializable {
  def createCounter(runtimeContext: RuntimeContext, name: String): Metric

  def createMeter(runtimeContext: RuntimeContext, name: String): Metric
}
