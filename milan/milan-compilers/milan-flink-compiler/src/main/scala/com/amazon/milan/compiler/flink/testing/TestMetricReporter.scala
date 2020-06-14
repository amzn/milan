package com.amazon.milan.compiler.flink.testing

import org.apache.flink.metrics._
import org.apache.flink.metrics.reporter.MetricReporter

import scala.collection.mutable


class TestMetricReporter extends MetricReporter {

  import TestMetricReporter._

  val ARG_IDENTIFIER: String = "identifier"

  @transient private var identifier: String = _

  override def open(metricConfig: MetricConfig): Unit = {
    this.identifier = metricConfig.getString(this.ARG_IDENTIFIER, "")
  }

  override def close(): Unit = {
  }

  override def notifyOfAddedMetric(metric: Metric, metricName: String, group: MetricGroup): Unit = {
    // Only add metrics that contain the identifier
    if (!metricName.contains(this.identifier)) return

    metric match {
      case counter: Counter => counterMetrics(metricName) = counter
      case meter: Meter => meterMetrics(metricName) = meter
      case histogram: Histogram => histogramMetrics(metricName) = histogram
      case _ =>
    }
  }

  override def notifyOfRemovedMetric(metric: Metric, metricName: String, group: MetricGroup): Unit = {
  }
}


object TestMetricReporter {
  val counterMetrics: mutable.Map[String, Counter] = mutable.Map[String, Counter]()
  val meterMetrics: mutable.Map[String, Meter] = mutable.Map[String, Meter]()
  val histogramMetrics: mutable.Map[String, Histogram] = mutable.Map[String, Histogram]()
}
