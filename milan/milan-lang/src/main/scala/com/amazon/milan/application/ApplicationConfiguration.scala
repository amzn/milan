package com.amazon.milan.application

import com.amazon.milan.application.state.DefaultStateStore
import com.amazon.milan.lang.{StateIdentifier, Stream}
import com.amazon.milan.types.LineageRecord


object ApplicationConfiguration {

  case class StreamSink(streamId: String, sink: DataSink[_])

  case class StreamMetric(streamId: String, metric: MetricDefinition[_])

}

import com.amazon.milan.application.ApplicationConfiguration._


/**
 * Contains the configuration of the data sources and sinks for an application.
 */
class ApplicationConfiguration {
  private var metricPrefix: String = ""

  var dataSources: Map[String, DataSource[_]] = Map.empty
  var dataSinks: List[StreamSink] = List.empty
  var lineageSinks: List[DataSink[LineageRecord]] = List.empty
  var metrics: List[StreamMetric] = List.empty
  var stateStores: Map[String, Map[String, StateStore]] = Map.empty

  /**
   * Sets the source of a data stream.
   *
   * @param stream A data stream reference.
   * @param source A DataSource representing the source of the data.
   */
  def setSource[T](stream: Stream[T], source: DataSource[T]): Unit =
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
  def addSink[T](stream: Stream[T], sink: DataSink[T]): Unit =
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
   * Configures state storage for an operation.
   *
   * @param stream A [[Stream]] corresponding to the operation to configure.
   * @param state  Identifies which of the operation's state stores is being configured.
   * @param store  A [[StateStore]] configuring the state storage for the operation.
   */
  def setStateStore(stream: Stream[_], state: StateIdentifier, store: StateStore): Unit =
    this.setStateStore(stream.streamId, state.stateId, store)

  /**
   * Configures state storage for an operation.
   *
   * @param operationId The ID of the stream corresponding to the operation to configure.
   * @param stateId     Identifies which of the operation's state stores is being configured.
   * @param store       A [[StateStore]] configuring the state storage for the operation.
   */
  def setStateStore(operationId: String, stateId: String, store: StateStore): Unit = {
    this.stateStores = this.stateStores +
      (operationId -> (this.stateStores.getOrElse(operationId, Map.empty) + (stateId -> store)))
  }

  /**
   * Gets the state store for a stream.
   *
   * @param operationId The ID of the stream corresponding to the operation.
   * @param stateId     Identifies which of the operation's state stores is being retrieved.
   * @return A [[StateStore]] object describing the state store configuration for the stream.
   */
  def getStateStore(operationId: String, stateId: String): StateStore = {
    this.stateStores.getOrElse(operationId, Map.empty)
      .getOrElse(stateId, new DefaultStateStore)
  }

  /**
   * Adds a metric for a stream.
   *
   * @param stream           A data stream reference.
   * @param metricDefinition A metric definition representing the metric to be added to the stream.
   */
  def addMetric[T](stream: Stream[T], metricDefinition: MetricDefinition[T]): Unit =
    this.metrics = this.metrics :+ StreamMetric(stream.streamId, metricDefinition)

  def setMetricPrefix(prefix: String): Unit = this.metricPrefix = prefix

  def getMetricPrefix: String = this.metricPrefix

  override def equals(obj: Any): Boolean = obj match {
    case o: ApplicationConfiguration =>
      this.dataSources.equals(o.dataSources) &&
        this.dataSinks.equals(o.dataSinks) &&
        this.metrics.equals(o.metrics) &&
        this.lineageSinks.equals(o.lineageSinks) &&
        this.stateStores.equals(o.stateStores) &&
        this.metrics.equals(o.metrics) &&
        this.metricPrefix == o.metricPrefix

    case _ =>
      false
  }
}
