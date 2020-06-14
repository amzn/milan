package com.amazon.milan.compiler.flink.testing

import com.amazon.milan.compiler.scala.RuntimeEvaluator
import com.amazon.milan.compiler.flink.types.ArrayRecord
import com.amazon.milan.typeutil.TypeDescriptor
import com.amazon.milan.{Id, application}
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.slf4j.LoggerFactory


/**
 * An sink function that stores the output in shared memory so that it can be accessed by anyone with the sink ID.
 *
 * @tparam T The type of items collected by the sink.
 */
class SingletonMemorySinkFunction[T](sinkId: String) extends SinkFunction[T] {
  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  def this() {
    this(Id.newId())
  }

  override def invoke(value: T): Unit = {
    this.logger.info(s"SingletonMemorySink ${this.sinkId} collecting value.")
    application.sinks.SingletonMemorySink.add(this.sinkId, value)
  }

  /**
   * Gets the number of records that have been sent to the sink.
   *
   * @return The record count.
   */
  def getRecordCount: Int = application.sinks.SingletonMemorySink.getBuffer(this.sinkId).size

  /**
   * Gets the values that have been added to the sink.
   *
   * @return A list of the values.
   */
  def getValues: List[T] = {
    application.sinks.SingletonMemorySink.getBuffer[T](this.sinkId).map(_.value).toList
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
