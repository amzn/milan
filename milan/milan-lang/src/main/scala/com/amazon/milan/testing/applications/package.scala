package com.amazon.milan.testing

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.application.sources.ListDataSource
import com.amazon.milan.lang.Stream
import com.amazon.milan.typeutil.TypeDescriptor

import scala.language.implicitConversions


package object applications {

  implicit class ApplicationConfigurationExtensions(config: ApplicationConfiguration) {
    /**
     * Sets the source of a stream to a list of items.
     *
     * @param stream     The stream whose source is being set.
     * @param runForever Whether the source should continue running the the items have all been sent to the program.
     *                   If true, the source will idle until cancelled.
     * @param values     The values to use as the data source.
     * @tparam T The type of values.
     */
    def setListSource[T](stream: Stream[T], runForever: Boolean, values: T*): Unit = {
      this.config.setSource(stream, new ListDataSource[T](values.toList, runForever)(stream.recordType))
    }

    /**
     * Sets the source of a stream to a list of items.
     *
     * @param stream The stream whose source is being set.
     * @param values The values to use as the data source.
     * @tparam T The type of values.
     */
    def setListSource[T](stream: Stream[T], values: T*): Unit = {
      this.setListSource(stream, false, values: _*)
    }

    /**
     * Sets the source of a stream to a list of items.
     *
     * @param streamId   The ID of the stream whose source is being set.
     * @param runForever Whether the source should continue running the the items have all been sent to the program.
     *                   If true, the source will idle until cancelled.
     * @param values     The values to use as the data source.
     * @tparam T The type of values.
     */
    def setListSource[T: TypeDescriptor](streamId: String, runForever: Boolean, values: T*): Unit = {
      this.config.setSource(streamId, new ListDataSource[T](values.toList, runForever))
    }

    /**
     * Sets the source of a stream to a list of items.
     *
     * @param streamId The ID of the stream whose source is being set.
     * @param values   The values to use as the data source.
     * @tparam T The type of values.
     */
    def setListSource[T: TypeDescriptor](streamId: String, values: T*): Unit = {
      this.setListSource(streamId, false, values: _*)
    }
  }

}
