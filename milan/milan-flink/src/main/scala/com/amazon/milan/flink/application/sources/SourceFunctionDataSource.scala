package com.amazon.milan.flink.application.sources

import com.amazon.milan.flink.application.FlinkDataSource
import com.amazon.milan.typeutil.TypeDescriptor
import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction


/**
 * A [[FlinkDataSource]] implementation that wraps a Flink SourceFunction.
 *
 * @param sourceFunction The source function to wrap.
 * @tparam T The type of record objects.
 */
class SourceFunctionDataSource[T: TypeInformation](val sourceFunction: SourceFunction[T],
                                                   val maxParallelism: Int = 0)
  extends FlinkDataSource[T] {

  private val hashCodeValue = HashCodeBuilder.reflectionHashCode(this)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    // This class should not be deserialized by GenericTypedJsonDeserializer, which means this should not be called.
    throw new NotImplementedError("You shouldn't be here.")
  }

  override def addDataSource(env: StreamExecutionEnvironment): DataStreamSource[T] = {
    val stream = env.addSource(this.sourceFunction)

    if (this.maxParallelism > 0) {
      stream.setMaxParallelism(this.maxParallelism)
    }

    stream
  }

  override def hashCode(): Int = this.hashCodeValue

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: SourceFunctionDataSource[T] =>
        this.sourceFunction.equals(o.sourceFunction) &&
          this.maxParallelism == o.maxParallelism

      case _ =>
        false
    }
  }
}
