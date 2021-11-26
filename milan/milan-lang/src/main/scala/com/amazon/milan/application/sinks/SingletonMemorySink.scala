package com.amazon.milan.application.sinks

import com.amazon.milan.Id
import com.amazon.milan.application.DataSink
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

import java.time.{Duration, Instant}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.function
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.TimeoutException


object SingletonMemorySink {
  private val values = new ConcurrentHashMap[String, ArrayBuffer[MemorySinkRecord[_]]]()
  private val nextSeqNum = new mutable.HashMap[String, Int]()
  private val locks = new ConcurrentHashMap[String, Object]()
  private val createLocker = new java.util.function.Function[String, Object] {
    override def apply(t: String): AnyRef = new Object()
  }

  /**
   * Adds a value to the collection of values for a sink.
   *
   * @param sinkId The sink ID.
   * @param item   The item to add.
   * @tparam T The type of the item.
   */
  def add[T](sinkId: String, item: T): Unit = {
    // We want to increment the sequence number and add the item in one atomic operation.
    // We'll use scala's synchronize operation for this, which requires an object to sync on.
    val locker = this.locks.computeIfAbsent(sinkId, createLocker)

    locker.synchronized {
      val seqNum = this.nextSeqNum.getOrElseUpdate(sinkId, 1)

      val record = new MemorySinkRecord[T](seqNum.toString, Instant.now(), item)
      getBuffer(sinkId).append(record)

      this.nextSeqNum.put(sinkId, seqNum + 1)
    }
  }

  /**
   * Gets the values for a sink.
   *
   * @param sinkId The sink ID.
   * @return The [[ConcurrentLinkedQueue]] used for collecting the sink values.
   */
  def getBuffer[T](sinkId: String): ArrayBuffer[MemorySinkRecord[T]] = {
    this.values.computeIfAbsent(sinkId, makeCreateBufferFunction[T]).asInstanceOf[ArrayBuffer[MemorySinkRecord[T]]]
  }

  private def makeCreateBufferFunction[T]: java.util.function.Function[String, ArrayBuffer[MemorySinkRecord[_]]] =
    new function.Function[String, ArrayBuffer[MemorySinkRecord[_]]] {
      override def apply(t: String): ArrayBuffer[MemorySinkRecord[_]] =
        (new ArrayBuffer[MemorySinkRecord[T]]()).asInstanceOf[ArrayBuffer[MemorySinkRecord[_]]]
    }
}


/**
 * An sink function that stores the output in shared memory so that it can be accessed by any instance of the sink.
 * This is designed to help with testing, and is only appropriate when the Flink application executes in a single
 * process.
 *
 * @tparam T The type of items collected by the sink.
 */
@JsonSerialize
@JsonDeserialize
class SingletonMemorySink[T: TypeDescriptor](val sinkId: String) extends DataSink[T] {
  private var recordTypeDescriptor = implicitly[TypeDescriptor[T]]

  override def getGenericArguments: List[TypeDescriptor[_]] = List(this.recordTypeDescriptor)

  override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = {
    this.recordTypeDescriptor = genericArgs.head.asInstanceOf[TypeDescriptor[T]]
  }

  def this() {
    this(Id.newId())
  }

  /**
   * Gets whether any records have been sent to the sink.
   *
   * @return True if any values have been sent to the sink, otherwise false.
   */
  @JsonIgnore
  def hasValues: Boolean = SingletonMemorySink.getBuffer(this.sinkId).nonEmpty

  /**
   * Gets the number of records that have been sent to the sink.
   *
   * @return The record count.
   */
  @JsonIgnore
  def getRecordCount: Int = SingletonMemorySink.getBuffer(this.sinkId).size

  @JsonIgnore
  def getValues: List[T] = {
    SingletonMemorySink.getBuffer[T](this.sinkId).map(_.value).toList
  }

  @JsonIgnore
  def getRecords: List[MemorySinkRecord[T]] = {
    SingletonMemorySink.getBuffer[T](this.sinkId).toList
  }

  def waitForItems(itemCount: Int, timeout: Duration = null): Unit = {
    val endTime = if (timeout == null) Instant.MAX else Instant.now().plus(timeout)

    while (SingletonMemorySink.getBuffer(this.sinkId).size < itemCount) {
      if (Instant.now().isAfter(endTime)) {
        throw new TimeoutException()
      }

      Thread.sleep(1)
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: SingletonMemorySink[_] =>
        this.sinkId.equals(o.sinkId)

      case _ =>
        false
    }
  }
}


class MemorySinkRecord[T](val seqNum: String, val createdTime: Instant, val value: T) extends Serializable
