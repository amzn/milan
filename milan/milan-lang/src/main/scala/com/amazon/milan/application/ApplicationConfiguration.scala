package com.amazon.milan.application

import com.amazon.milan.lang.Stream
import com.amazon.milan.types.LineageRecord


object ApplicationConfiguration {

  case class StreamSink(streamId: String, sink: DataSink[_])

  case class StreamMetric(streamId: String, metric: MetricDefinition[_])

}

import com.amazon.milan.application.ApplicationConfiguration._


/**
 * Configures the data sources and sinks for an application.
 * One of these will be used for each running instance of an application.
 */
class ApplicationConfiguration {
  private var metricPrefix: String = ""

  var dataSources: Map[String, DataSource[_]] = Map()
  var dataSinks: List[StreamSink] = List()
  var lineageSinks: List[DataSink[LineageRecord]] = List()
  var metrics: List[StreamMetric] = List()

  /**
   * Sets the source of a data stream.
   *
   * @param stream A data stream reference.
   * @param source A DataSource representing the source of the data.
   */
  def setSource[T](stream: Stream[T, _], source: DataSource[T]): Unit =
    this.setSource(stream.streamId, source)

  /**
   * Sets the source of a data stream.
   *
   * @param streamId The ID of a stream.
   * @param source   A DataSource representing the source of the data.
   */
  def setSource(streamId: String, source: DataSource[_]): Unit =
    this.dataSources = this.dataSources + (streamId -> source)

  /**
   * Adds a sink for stream data.
   *
   * @param stream A stream.
   * @param sink   The sink to add for the stream data.
   */
  def addSink[T](stream: Stream[T, _], sink: DataSink[T]): Unit =
    this.addSink(stream.streamId, sink)

  /**
   * Adds a sink for stream data.
   *
   * @param streamId The ID of a stream.
   * @param sink     The sink to add for the stream data.
   */
  def addSink(streamId: String, sink: DataSink[_]): Unit =
    this.dataSinks = this.dataSinks :+ StreamSink(streamId, sink)

  /**
   * Adds a sink for record lineage data.
   *
   * @param sink A data sink that accepts lineage records.
   */
  def addLineageSink(sink: DataSink[LineageRecord]): Unit =
    this.lineageSinks = this.lineageSinks :+ sink

  /**
   * Adds a metric for a stream.
   *
   * @param stream           A data stream reference.
   * @param metricDefinition A metric definition representing the metric to be added to the stream.
   */
  def addMetric[T](stream: Stream[T, _], metricDefinition: MetricDefinition[T]): Unit =
    this.metrics = this.metrics :+ StreamMetric(stream.streamId, metricDefinition)

  def setMetricPrefix(prefix: String): Unit = this.metricPrefix = prefix

  def getMetricPrefix: String = this.metricPrefix

  override def equals(obj: Any): Boolean = obj match {
    case o: ApplicationConfiguration =>
      this.dataSources.equals(o.dataSources) &&
        this.dataSinks.equals(o.dataSinks) &&
        this.metrics.equals(o.metrics) &&
        this.lineageSinks.equals(o.lineageSinks) &&
        this.metrics.equals(o.metrics) &&
        this.metricPrefix == o.metricPrefix

    case _ =>
      false
  }
}
