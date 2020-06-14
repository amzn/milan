package com.amazon.milan.compiler.flink.metrics

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper


class OperatorMetricFactory(metricPrefix: String, operatorName: String) extends MetricFactory {
  private val prefix =
    if (metricPrefix != null && metricPrefix.nonEmpty)
      s"$metricPrefix.$operatorName-"
    else
      operatorName

  override def createCounter(runtimeContext: RuntimeContext, name: String): Metric = {
    val metricGroup = runtimeContext.getMetricGroup
    val metricName = this.prefix + name
    new CounterWrapper(metricGroup.counter(metricName))
  }

  override def createMeter(runtimeContext: RuntimeContext, name: String): Metric = {
    val metricGroup = runtimeContext.getMetricGroup
    val meterWrapper = new DropwizardMeterWrapper(new com.codahale.metrics.Meter())
    val metricName = this.prefix + name
    new MeterWrapper(metricGroup.meter(metricName, meterWrapper))
  }
}
