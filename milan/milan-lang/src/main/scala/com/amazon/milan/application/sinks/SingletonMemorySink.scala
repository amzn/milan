package com.amazon.milan.application.sinks

import java.time.{Duration, Instant}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import com.amazon.milan.Id
import com.amazon.milan.application.DataSink
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.TimeoutException


object SingletonMemorySink {
  private val values = new ConcurrentHashMap[String, ConcurrentLinkedQueue[Object]]()
  private val nextSeqNum = new mutable.HashMap[String, Int]()
  private val locks = new ConcurrentHashMap[String, Object]()

  private val createQueueFunction = new java.util.function.Function[String, ConcurrentLinkedQueue[Object]] {
    override def apply(v1: String): ConcurrentLinkedQueue[Object] = new ConcurrentLinkedQueue[Object]()
  }

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
      getValues(sinkId).add(record)

      this.nextSeqNum.put(sinkId, seqNum + 1)
    }
  }

  /**
   * Gets the values for a sink.
   *
   * @param sinkId The sink ID.
   * @return The [[ConcurrentLinkedQueue]] used for collecting the sink values.
   */
  def getValues(sinkId: String): ConcurrentLinkedQueue[Object] = {
    this.values.computeIfAbsent(sinkId, createQueueFunction)
  }
}


/**
 * An sink function that stores the output in shared memory so that it can be accessed by any instance of the sink.
 *
 * @tparam T The type of items collected by the sink.
 */
@JsonSerialize
@JsonDeserialize
class SingletonMemorySink[T: TypeDescriptor](val id: String) extends DataSink[T] {
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
  def hasValues: Boolean = !SingletonMemorySink.getValues(this.id).isEmpty

  /**
   * Gets the number of records that have been sent to the sink.
   *
   * @return The record count.
   */
  @JsonIgnore
  def getRecordCount: Int = SingletonMemorySink.getValues(this.id).size()

  @JsonIgnore
  def getValues: List[T] = {
    SingletonMemorySink.getValues(this.id).asScala.map(_.asInstanceOf[MemorySinkRecord[T]].value).toList
  }

  @JsonIgnore
  def getRecords: List[MemorySinkRecord[T]] = {
    SingletonMemorySink.getValues(this.id).asScala.map(_.asInstanceOf[MemorySinkRecord[T]]).toList
  }

  def waitForItems(itemCount: Int, timeout: Duration = null): Unit = {
    val endTime = if (timeout == null) Instant.MAX else Instant.now().plus(timeout)

    while (SingletonMemorySink.getValues(this.id).size < itemCount) {
      if (Instant.now().isAfter(endTime)) {
        throw new TimeoutException()
      }

      Thread.sleep(1)
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: SingletonMemorySink[_] =>
        this.id.equals(o.id)

      case _ =>
        false
    }
  }
}


class MemorySinkRecord[T](val seqNum: String, val createdTime: Instant, val value: T) extends Serializable
