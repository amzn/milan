package com.amazon.milan.flink.application.sinks

import com.amazon.milan.application.sinks.MemorySinkRecord
import com.amazon.milan.flink.RuntimeEvaluator
import com.amazon.milan.flink.application.FlinkDataSink
import com.amazon.milan.flink.types.ArrayRecord
import com.amazon.milan.typeutil.TypeDescriptor
import com.amazon.milan.{Id, application}
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


object FlinkSingletonMemorySink {
  def create[T: TypeDescriptor]: FlinkSingletonMemorySink[T] =
    new FlinkSingletonMemorySink[T](Id.newId(), implicitly[TypeDescriptor[T]])
}


/**
 * Implementation of the Milan SingletonMemorySink for Flink.
 *
 * @param id The ID of the sink.
 * @tparam T The type of objects that are written to the sink.
 */
@JsonDeserialize
class FlinkSingletonMemorySink[T](val id: String, var recordTypeDescriptor: TypeDescriptor[T]) extends FlinkDataSink[T] {
  @JsonCreator
  def this(id: String) {
    this(id, null)
  }

  def this(recordTypeDescriptor: TypeDescriptor[T]) {
    this(Id.newId(), recordTypeDescriptor)
  }

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }

  override def getSinkFunction: SinkFunction[_] = {
    if (this.recordTypeDescriptor.isTuple) {
      new TupleSingletonMemorySinkFunction[T](id, recordTypeDescriptor)
    }
    else {
      new SingletonMemorySinkFunction[T](this.id)
    }
  }

  /**
   * Gets the number of records that have been sent to the sink.
   *
   * @return The record count.
   */
  def getRecordCount: Int = application.sinks.SingletonMemorySink.getValues(this.id).size()

  /**
   * Gets the values that have been added to the sink.
   *
   * @return A list of the values.
   */
  def getValues: List[T] = {
    application.sinks.SingletonMemorySink.getValues(this.id).asScala.map(_.asInstanceOf[MemorySinkRecord[T]].value).toList
  }
}


/**
 * An sink function that stores the output in shared memory so that it can be accessed by anyone with the sink ID.
 *
 * @tparam T The type of items collected by the sink.
 */
class SingletonMemorySinkFunction[T](id: String) extends SinkFunction[T] {
  def this() {
    this(Id.newId())
  }

  override def invoke(value: T): Unit = {
    application.sinks.SingletonMemorySink.add(this.id, value)
  }

  /**
   * Gets the number of records that have been sent to the sink.
   *
   * @return The record count.
   */
  def getRecordCount: Int = application.sinks.SingletonMemorySink.getValues(this.id).size()

  /**
   * Gets the values that have been added to the sink.
   *
   * @return A list of the values.
   */
  def getValues: List[T] = {
    application.sinks.SingletonMemorySink.getValues(this.id).asScala.map(_.asInstanceOf[MemorySinkRecord[T]].value).toList
  }
}


/**
 * An sink function that stores the output in shared memory so that it can be accessed by anyone with the sink ID.
 *
 * @tparam T The type of items collected by the sink.
 */
class TupleSingletonMemorySinkFunction[T](id: String,
                                          typeDescriptor: TypeDescriptor[T])
  extends SinkFunction[ArrayRecord] {

  @transient private lazy val createInstanceFunc = this.compileCreateInstanceFunc()
  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  override def invoke(value: ArrayRecord): Unit = {
    // Convert the value to the expected Tuple type before adding it to the sink.
    val tupleValue = this.createInstanceFunc(value.values)
    application.sinks.SingletonMemorySink.add[T](this.id, tupleValue)
  }

  private def compileCreateInstanceFunc(): Array[Any] => T = {
    val eval = RuntimeEvaluator.instance

    val fieldTypeNames = this.typeDescriptor.genericArguments.map(_.fullName)

    // Create statements that get the tuple values from the list and cast them to the
    // expected type for the corresponding tuple element.
    val fieldValueGetters = fieldTypeNames.zipWithIndex.map {
      case (f, i) => s"values($i).asInstanceOf[$f]"
    }

    val fieldValuesStatement = fieldValueGetters.mkString(", ")
    val tupleCreationStatement = s"${this.typeDescriptor.typeName}($fieldValuesStatement)"

    this.logger.info(s"Compiling tuple creation function: $tupleCreationStatement")

    eval.createFunction[Array[Any], T](
      "values",
      "Array[Any]",
      tupleCreationStatement
    )
  }
}
