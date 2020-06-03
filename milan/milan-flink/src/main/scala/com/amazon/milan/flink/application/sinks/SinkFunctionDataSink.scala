package com.amazon.milan.flink.application.sinks

import com.amazon.milan.flink.application.FlinkDataSink
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.flink.streaming.api.functions.sink.SinkFunction


class SinkFunctionDataSink[T](sinkFunction: SinkFunction[T]) extends FlinkDataSink[T] {
  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    // This class should not be deserialized by GenericTypedJsonDeserializer, which means this should not be called.
    throw new NotImplementedError("You shouldn't be here.")
  }

  override def getSinkFunction: SinkFunction[T] = this.sinkFunction
}
