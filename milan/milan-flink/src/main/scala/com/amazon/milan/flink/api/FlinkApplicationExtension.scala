package com.amazon.milan.flink.api

import com.amazon.milan.flink.application.{FlinkDataSink, FlinkDataSource}


/**
 * Trait for classes that extend Milan Flink applications to provide data sources and sinks that are not available in
 * the core library.
 */
trait FlinkApplicationExtension {
  def extensionId: String

  def getFlinkDataSinkClass(typeName: String): Option[Class[_ <: FlinkDataSink[_]]]

  def getFlinkDataSourceClass[T](typeName: String): Option[Class[_ <: FlinkDataSource[_]]]
}
