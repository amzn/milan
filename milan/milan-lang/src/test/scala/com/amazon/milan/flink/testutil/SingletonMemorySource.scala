package com.amazon.milan.flink.testutil

import java.time.{Duration, Instant}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import com.amazon.milan.Id._
import com.typesafe.scalalogging.Logger
import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.language.postfixOps


object SingletonMemorySource {
  private val items = new ConcurrentHashMap[String, ConcurrentLinkedQueue[Any]]()

  private val stopWhenEmpty = new ConcurrentHashMap[String, Boolean]()

  private val finishedSources = new ConcurrentHashMap[String, Boolean]()

  private val waitingFor = new ConcurrentHashMap[String, ConcurrentLinkedQueue[(Future[_], Duration)]]()

  /**
   * Creates a [[SingletonMemorySource]]`[`T`]` containing the specified items.
   *
   * @param items The items to produce from the source.
   * @tparam T The type of the items.
   * @return A [[SingletonMemorySource]]`[`T`]` that produces the items.
   */
  def ofItems[T: TypeInformation](items: T*): SingletonMemorySource[T] = {
    new SingletonMemorySource[T](items)
  }

  /**
   * Creates a [[SingletonMemorySource]]`[`T`]` containing the specified items.
   *
   * @param items The items to produce from the source.
   * @tparam T The type of the items.
   * @return A [[SingletonMemorySource]]`[`T`]` that produces the items.
   */
  def ofItems[T: TypeInformation](stopRunningWhenEmpty: Boolean, items: T*): SingletonMemorySource[T] = {
    new SingletonMemorySource[T](items, stopRunningWhenEmpty)
  }
}


/**
 * A Flink SourceFunction that uses shared memory as the source of items.
 * Creating an instance in a test method and adding items to it will cause those same items to be output from the source
 * when it is run by Flink.
 * Instances of this class are only suitable for use when the entire application will run in the same process in which
 * the instance was created. If it is serialized and deserialized in another process then the deserialized instance
 * will not have access to the objects, because they are stored in shared memory in the original process.
 *
 * @param stopRunningWhenEmpty Specifies whether the run() method exits when the queue of items is empty.
 * @tparam T The type of item produced by the source.
 */
class SingletonMemorySource[T: TypeInformation](stopRunningWhenEmpty: Boolean)
  extends RichSourceFunction[T]
    with ResultTypeQueryable[T]
    with CheckpointedFunction {

  val id: String = newId()
  private val hashCodeValue = HashCodeBuilder.reflectionHashCode(this)
  private val itemTypeInformation = implicitly[TypeInformation[T]]

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(this.getClass))
  @transient private var running: Boolean = false
  @transient private var positionState: ValueState[Int] = _
  @transient private var position = 0

  SingletonMemorySource.items.put(this.id, new ConcurrentLinkedQueue[Any]())
  SingletonMemorySource.stopWhenEmpty.put(this.id, stopRunningWhenEmpty)

  def this(items: TraversableOnce[T], stopRunningWhenEmpty: Boolean = true) {
    this(stopRunningWhenEmpty)
    items.foreach(this.add)
  }

  def add(item: T): Unit = {
    SingletonMemorySource.items.get(this.id).add(item)
  }

  def waitFor(future: Future[_], atMost: Duration): Unit = {
    val items = SingletonMemorySource.waitingFor.computeIfAbsent(this.id, _ => new ConcurrentLinkedQueue[(Future[_], Duration)]())
    items.add((future, atMost))
  }

  def stopWhenEmpty: Boolean = SingletonMemorySource.stopWhenEmpty.get(this.id)

  def stopWhenEmpty_=(value: Boolean): Unit = SingletonMemorySource.stopWhenEmpty.put(this.id, value)

  def isEmpty: Boolean = SingletonMemorySource.items.get(this.id).isEmpty

  def awaitEmpty(implicit c: ExecutionContext): Future[Unit] = {
    Future {
      blocking {
        while (!isEmpty) {
          Thread.sleep(10)
        }
      }
    }
  }

  def waitForEmpty(timeout: Duration = null): Unit = {
    val startTime = Instant.now()
    val endTime = if (timeout == null) Instant.MAX else startTime.plus(timeout)

    while (!this.isEmpty) {
      if (Instant.now().isAfter(endTime)) {
        throw new TimeoutException()
      }

      Thread.sleep(1)
    }

    val runTime = Duration.between(startTime, Instant.now())
    this.logger.info(s"Waited ${runTime.toMillis} ms for test source to be empty.")
  }

  override def run(sourceContext: SourceFunction.SourceContext[T]): Unit = {
    this.running = true

    logger.info(s"Starting SingletonMemorySource of ${this.itemTypeInformation.toString}.")

    val items = SingletonMemorySource.items.get(this.id)

    for (_ <- 1 to this.position) {
      items.poll()
    }

    val waitingFor = SingletonMemorySource.waitingFor.getOrDefault(this.id, null)
    if (waitingFor != null && !waitingFor.isEmpty) {
      this.logger.info(s"SingletonMemorySource ${this.id} waiting for other sources to be empty.")
      waitingFor.asScala.foreach { case (f, d) => Await.ready(f, duration.Duration(d.toMillis, duration.MILLISECONDS)) }
      this.logger.info(s"SingletonMemorySource ${this.id} done waiting.")
    }

    while (this.running) {
      val item = items.poll()

      if (item != null) {
        this.logger.info(s"SingletonMemorySource ${this.id} collecting item at position ${this.position}.")

        sourceContext.getCheckpointLock.synchronized {
          sourceContext.collect(item.asInstanceOf[T])
          this.position += 1
        }
      }
      else if (this.stopWhenEmpty) {
        this.logger.info(s"SingletonMemorySource ${this.id} is empty and stopping at position ${this.position}.")
        this.running = false
      }
      else {
        Thread.sleep(1)
      }
    }

    SingletonMemorySource.finishedSources.put(this.id, true)

    logger.info(s"Stopping SingletonMemorySource of ${this.itemTypeInformation.toString}.")
  }

  override def cancel(): Unit = {
    this.logger.info(s"Cancelling SingletonMemorySource of ${this.itemTypeInformation.toString}.")
    this.running = false
  }

  override def getProducedType: TypeInformation[T] = this.itemTypeInformation

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val positionDescriptor = new ValueStateDescriptor[Int]("position", TypeInformation.of[Int](classOf[Int]))

    if (context.getKeyedStateStore != null) {
      this.positionState = context.getKeyedStateStore.getState(positionDescriptor)
      this.position = this.positionState.value()
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    if (this.positionState != null) {
      this.positionState.update(this.position)
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: SingletonMemorySource[T] =>
        this.id.equals(o.id)

      case _ =>
        false
    }
  }

  override def hashCode(): Int = this.hashCodeValue
}
