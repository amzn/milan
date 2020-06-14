package com.amazon.milan.compiler.flink.metrics

import org.apache.flink.api.common.functions.RuntimeContext


class SuffixAppendingMetricFactory(innerFactory: MetricFactory, suffix: String) extends MetricFactory {
  override def createCounter(runtimeContext: RuntimeContext, name: String): Metric =
    this.innerFactory.createCounter(runtimeContext, name + suffix)

  override def createMeter(runtimeContext: RuntimeContext, name: String): Metric =
    this.innerFactory.createMeter(runtimeContext, name + suffix)
}
