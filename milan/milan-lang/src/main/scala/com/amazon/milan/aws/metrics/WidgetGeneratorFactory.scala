package com.amazon.milan.aws.metrics


/**
 * Factory for producing CloudFormation Widget generators for metrics.
 *
 * @param namespace             CloudFormation namespace of metrics.
 * @param region                AWS Region that metrics are in.
 * @param host                  The IP address of the host sending the metrics.
 * @param workerHosts           The IP address of all the worker hosts.
 * @param applicationInstanceId The application instance ID.
 * @param maxParallelism        The maximum parallelism of the task.
 * @param periodSec             The period of the dashboard metrics in seconds.
 * @param liveData              Whether to show the last data point which may not be fully aggregated.
 * @param stacked               Whether to use a stacked line graph.
 * @param metricType            The type of the metric.
 */
class WidgetGeneratorFactory(val namespace: String, val region: String, val host: String, val workerHosts: List[String],
                             val applicationInstanceId: String, val maxParallelism: Int, val periodSec: Int,
                             val liveData: Boolean, val stacked: Boolean, val metricType: String) {

  /**
   * Get the [[LineWidgetJsonGenerator]] objects for a [[CompiledMetric]].
   *
   * @param compiledMetric The metric to get the widget generators for.
   * @return List of widget generators.
   */
  def getMetricWidget(compiledMetric: CompiledMetric): List[LineWidgetJsonGenerator] = {
    compiledMetric.metricType match {
      case MetricType.Meter => this.getMeterLineWidget(compiledMetric.name, compiledMetric.operatorName)
      case MetricType.Counter => this.getCounterLineWidget(compiledMetric.name, compiledMetric.operatorName)
      case MetricType.Histogram => this.getHistogramLineWidget(compiledMetric.name, compiledMetric.operatorName)
    }
  }

  /**
   * Get [[LineWidgetJsonGenerator]] objects for a Meter.
   *
   * @param metricName   Name of the metric.
   * @param operatorName Name of the operator the Meter is operating on.
   * @return List of widget generators.
   */
  private def getMeterLineWidget(metricName: String, operatorName: String): List[LineWidgetJsonGenerator] = {
    this.getFlinkLineWidgets(s"${metricName}_rate", operatorName, "Events/second")
  }

  /**
   * Get line widget generators for metrics.
   *
   * @param metricName   Name of the metric.
   * @param operatorName Name of the operator the Meter is operating on.
   * @param axisLabel    Label for the y axis.
   * @return List of widget generators.
   */
  private def getFlinkLineWidgets(metricName: String, operatorName: String, axisLabel: String):
  List[LineWidgetJsonGenerator] = {
    // Create a widget for each workerHost and each parallelism
    // For now we will only do for subtask 0
    this.workerHosts.map(workerHost =>
      new LineWidgetJsonGenerator(
        this.namespace,
        s"${workerHost}_${this.applicationInstanceId}_${operatorName}_0_$metricName",
        this.metricType,
        this.stacked,
        this.region,
        this.periodSec,
        s"${workerHost}_${operatorName}_0_$metricName",
        axisLabel,
        this.host,
        this.liveData
      )
    )
  }

  /**
   * Get [[LineWidgetJsonGenerator]] objects for a Counter.
   *
   * @param metricName   Name of the metric.
   * @param operatorName Name of the operator the Counter is operating on.
   * @return List of widget generators.
   */
  private def getCounterLineWidget(metricName: String, operatorName: String): List[LineWidgetJsonGenerator] = {
    this.getFlinkLineWidgets(metricName, operatorName, "Count")
  }

  /**
   * Get [[LineWidgetJsonGenerator]] objects for a Histogram.
   *
   * @param metricName   Name of the metric.
   * @param operatorName Name of the operator the Histogram is operating on.
   * @return List of widget generators.
   */
  private def getHistogramLineWidget(metricName: String, operatorName: String): List[LineWidgetJsonGenerator] = {
    val suffix = List("max", "min", "mean", "stddev", "p50", "p75", "p95", "p98", "p99", "p999")
    val metricNames = suffix.map(s => s"${metricName}_$s")
    metricNames.flatMap(name => this.getFlinkLineWidgets(name, operatorName, ""))
  }
}
