package com.amazon.milan.flink.testing

import java.time.Duration
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import com.amazon.milan.Id._
import com.amazon.milan.flink.application.FlinkDataSink
import com.amazon.milan.flink.testing
import com.amazon.milan.typeutil.TypeDescriptor
import com.typesafe.scalalogging.Logger
import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
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

  /**
   * A data sink that adds items to a memory source.
   */
  class DataSink[T](sourceId: String) extends FlinkDataSink[T] with SinkFunction[T] {
    override def getSinkFunction: SinkFunction[_] = this

    override def setGenericArguments(genericArgs: List[TypeDescriptor[_]]): Unit = ()

    override def invoke(value: T): Unit = {
      SingletonMemorySource.items.get(this.sourceId).add(value)
    }
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
class SingletonMemorySource[T: TypeInformation](stopRunningWhenEmpty: Boolean, val sourceId: String)
  extends RichSourceFunction[T]
    with ResultTypeQueryable[T]
    with CheckpointedFunction {

  private val hashCodeValue = HashCodeBuilder.reflectionHashCode(this)
  private val itemTypeInformation = implicitly[TypeInformation[T]]

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(this.getClass))
  @transient private var running: Boolean = false
  @transient private var positionState: ValueState[Int] = _
  @transient private var position = 0

  SingletonMemorySource.items.put(this.sourceId, new ConcurrentLinkedQueue[Any]())
  SingletonMemorySource.stopWhenEmpty.put(this.sourceId, stopRunningWhenEmpty)

  def this(stopRunningWhenEmpty: Boolean) {
    this(stopRunningWhenEmpty, newId())
  }

  def this(items: TraversableOnce[T], stopRunningWhenEmpty: Boolean = true, sourceId: Option[String] = None) {
    this(stopRunningWhenEmpty, sourceId.fold(newId())(s => s))
    items.foreach(this.add)
  }

  /**
   * Adds a item to the source.
   */
  def add(item: T): Unit = {
    SingletonMemorySource.items.get(this.sourceId).add(item)
  }

  /**
   * Instructs the source to not collect any of its items until after a future completes.
   *
   * @param future The future to wait for.
   * @param atMost The maximum duration of time to wait.
   */
  def waitFor(future: Future[_], atMost: Duration): Unit = {
    val items = SingletonMemorySource.waitingFor.computeIfAbsent(this.sourceId, _ => new ConcurrentLinkedQueue[(Future[_], Duration)]())
    items.add((future, atMost))
  }

  /**
   * Gets whether the source will stop when no items are remaining to be collected.
   */
  def stopWhenEmpty: Boolean = SingletonMemorySource.stopWhenEmpty.get(this.sourceId)

  /**
   * Sets whether the source will stop when no items are remaining to be collected.
   */
  def stopWhenEmpty_=(value: Boolean): Unit = SingletonMemorySource.stopWhenEmpty.put(this.sourceId, value)

  /**
   * Gets whether the source has any remaining items to be collected.
   */
  def isEmpty: Boolean = SingletonMemorySource.items.get(this.sourceId).isEmpty

  /**
   * Gets a [[Future]] that completes when the source has collected all items.
   */
  def awaitEmpty(implicit c: ExecutionContext): Future[Unit] = {
    Future {
      blocking {
        while (!isEmpty) {
          Thread.sleep(10)
        }
      }
    }
  }

  /**
   * Gets a [[FlinkDataSink]] that adds items to this data source when invoked.
   */
  def createSink(): FlinkDataSink[T] = {
    new testing.SingletonMemorySource.DataSink[T](this.sourceId)
  }

  override def run(sourceContext: SourceFunction.SourceContext[T]): Unit = {
    this.running = true

    logger.info(s"Starting SingletonMemorySource of ${this.itemTypeInformation.toString}.")

    val items = SingletonMemorySource.items.get(this.sourceId)

    for (_ <- 1 to this.position) {
      items.poll()
    }

    val waitingFor = SingletonMemorySource.waitingFor.getOrDefault(this.sourceId, null)
    if (waitingFor != null && !waitingFor.isEmpty) {
      this.logger.info(s"SingletonMemorySource ${this.sourceId} waiting for other sources to be empty.")
      waitingFor.asScala.foreach { case (f, d) => Await.ready(f, duration.Duration(d.toMillis, duration.MILLISECONDS)) }
      this.logger.info(s"SingletonMemorySource ${this.sourceId} done waiting.")
    }

    while (this.running) {
      val item = items.poll()

      if (item != null) {
        this.logger.info(s"SingletonMemorySource ${this.sourceId} collecting item at position ${this.position}.")

        sourceContext.getCheckpointLock.synchronized {
          sourceContext.collect(item.asInstanceOf[T])
          this.position += 1
        }
      }
      else if (this.stopWhenEmpty) {
        this.logger.info(s"SingletonMemorySource ${this.sourceId} is empty and stopping at position ${this.position}.")
        this.running = false
      }
      else {
        Thread.sleep(1)
      }
    }

    SingletonMemorySource.finishedSources.put(this.sourceId, true)

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
        this.sourceId.equals(o.sourceId)

      case _ =>
        false
    }
  }

  override def hashCode(): Int = this.hashCodeValue
}
