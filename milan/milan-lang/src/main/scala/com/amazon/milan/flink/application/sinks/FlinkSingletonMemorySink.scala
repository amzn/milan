package com.amazon.milan.flink.application.sinks

import java.time.Duration

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

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._


object FlinkSingletonMemorySink {
  def create[T: TypeDescriptor](sinkId: String): FlinkSingletonMemorySink[T] =
    new FlinkSingletonMemorySink[T](sinkId, implicitly[TypeDescriptor[T]])

  def create[T: TypeDescriptor]: FlinkSingletonMemorySink[T] =
    this.create[T](Id.newId())
}


/**
 * Implementation of the Milan SingletonMemorySink for Flink.
 *
 * @param sinkId The ID of the sink.
 * @tparam T The type of objects that are written to the sink.
 */
@JsonDeserialize
class FlinkSingletonMemorySink[T](val sinkId: String, var recordTypeDescriptor: TypeDescriptor[T]) extends FlinkDataSink[T] {
  @JsonCreator
  def this(sinkId: String) {
    this(sinkId, null)
  }

  def this(recordTypeDescriptor: TypeDescriptor[T]) {
    this(Id.newId(), recordTypeDescriptor)
  }

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }

  override def getSinkFunction: SinkFunction[_] = {
    if (this.recordTypeDescriptor.isTuple) {
      new TupleSingletonMemorySinkFunction[T](sinkId, recordTypeDescriptor)
    }
    else {
      new SingletonMemorySinkFunction[T](this.sinkId)
    }
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

  /**
   * Gets a [[BlockingQueue]] that can be used to poll for items that are added to the sink.
   */
  def createValueQueue: BlockingQueue = new BlockingQueue(this.sinkId)

  class BlockingQueue(sinkId: String) {
    private var nextIndex: Int = 0
    private val buffer = application.sinks.SingletonMemorySink.getBuffer[T](this.sinkId)

    /**
     * Gets the next unread item from the queue.
     *
     * @param maximumWait The maximum time to wait for a new item.
     */
    def poll(maximumWait: Duration): T = {
      val f = Future {
        blocking {
          while (this.nextIndex >= this.buffer.size) {
            Thread.sleep(5)
          }
          val value = this.buffer(this.nextIndex).value
          this.nextIndex += 1
          value
        }
      }

      Await.result(f, duration.Duration(maximumWait.toMillis, duration.MILLISECONDS))
    }
  }

}


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
