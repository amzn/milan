package com.amazon.milan.compiler.flink.testutil

import com.amazon.milan.compiler.flink.metrics.{Metric, MetricFactory}
import org.apache.flink.api.common.functions.RuntimeContext


object DoNothingMetricFactory extends MetricFactory {
  override def createCounter(runtimeContext: RuntimeContext, name: String): Metric = {
    DoNothingMetric
  }

  override def createMeter(runtimeContext: RuntimeContext, name: String): Metric = {
    DoNothingMetric
  }
}


object DoNothingMetric extends Metric {
  override def increment(): Unit = {
  }

  override def increment(amount: Long): Unit = {
  }
}
