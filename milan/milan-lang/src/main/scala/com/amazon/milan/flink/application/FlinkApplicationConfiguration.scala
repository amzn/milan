package com.amazon.milan.flink.application

import com.amazon.milan.application.ApplicationConfiguration.StreamMetric
import com.amazon.milan.application.MetricDefinition
import com.amazon.milan.lang.Stream
import com.amazon.milan.types.LineageRecord


object FlinkApplicationConfiguration {

  case class FlinkStreamSink(streamId: String, sink: FlinkDataSink[_])

}

import com.amazon.milan.flink.application.FlinkApplicationConfiguration._


/**
 * This class mirrors [[com.amazon.milan.application.ApplicationConfiguration]].
 * It has the same fields, but the field types are changed to the versions of those types that are used by the Milan
 * Flink compiler.
 */
class FlinkApplicationConfiguration(var dataSources: Map[String, FlinkDataSource[_]],
                                    var dataSinks: List[FlinkStreamSink],
                                    var lineageSinks: List[FlinkDataSink[LineageRecord]],
                                    var metrics: List[StreamMetric],
                                    val metricPrefix: String) {
  def this() {
    this(Map(), List(), List(), List(), "")
  }

  /**
   * Sets the source of a data stream.
   *
   * @param stream A data stream reference.
   * @param source A DataSource representing the source of the data.
   */
  def setSource[T](stream: Stream[T, _], source: FlinkDataSource[T]): Unit =
    this.dataSources = this.dataSources + (stream.streamId -> source)

  /**
   * Gets the data source for a stream.
   *
   * @param streamId A stream ID.
   * @return The data source defined for the stream with the specified ID.
   */
  def getSource(streamId: String): FlinkDataSource[_] = this.dataSources(streamId)

  /**
   * Gets any metrics defined for a graph node.
   *
   * @param nodeId The ID of a graph node.
   * @return A sequence of the metrics defined for the stream.
   */
  def getMetricsForNode(nodeId: String): Seq[MetricDefinition[_]] =
    this.metrics.filter(_.streamId == nodeId).map(_.metric)

  /**
   * Adds a sink for stream data.
   *
   * @param stream A stream.
   * @param sink   The sink to add for the stream data.
   */
  def addSink[T](stream: Stream[T, _], sink: FlinkDataSink[T]): Unit =
    this.dataSinks = this.dataSinks :+ FlinkStreamSink(stream.streamId, sink)

  /**
   * Adds a sink for record lineage data.
   *
   * @param sink A data sink that accepts lineage records.
   */
  def addLineageSink(sink: FlinkDataSink[LineageRecord]): Unit =
    this.lineageSinks = this.lineageSinks :+ sink

  /**
   * Adds a metric for a stream.
   *
   * @param stream           A data stream reference.
   * @param metricDefinition A metric definition representing the metric to be added to the stream.
   */
  def addMetricDefinition[T](stream: Stream[T, _], metricDefinition: MetricDefinition[T]): Unit =
    this.metrics = this.metrics :+ StreamMetric(stream.streamId, metricDefinition)
}
