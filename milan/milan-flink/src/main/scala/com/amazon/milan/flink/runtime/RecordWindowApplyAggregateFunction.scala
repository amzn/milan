package com.amazon.milan.flink.runtime

import java.util

import com.amazon.milan.flink.types.{PriorityQueueTypeInformation, RecordWrapper, RecordWrapperTypeInformation}
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{MapTypeInfo, ResultTypeQueryable}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable


class SequenceNumberOrdering[T >: Null, TKey >: Null <: Product] extends Ordering[RecordWrapper[T, TKey]] {
  override def compare(x: RecordWrapper[T, TKey], y: RecordWrapper[T, TKey]): Int = {
    y.sequenceNumber.compareTo(x.sequenceNumber)
  }
}


/**
 * Base class for [[AggregateFunction]] classes that implement a .recordWindow().apply() operation.
 *
 * @param windowSize       The number of records in the window, per key.
 * @param inputTypeInfo    [[TypeInformation]] for the input record value type.
 * @param inputKeyTypeInfo [[TypeInformation]] for the input record key type.
 * @param keyTypeInfo      [[TypeInformation]] for the aggregation key type.
 * @param outputTypeInfo   [[TypeInformation]] for the output record value type.
 * @tparam TIn    The input record value type.
 * @tparam TInKey The input record key type.
 * @tparam TKey   The key type used by the aggregation.
 * @tparam TOut   The output record value type.
 */
abstract class RecordWindowApplyAggregateFunction[TIn >: Null, TInKey >: Null <: Product, TKey, TOut >: Null](windowSize: Int,
                                                                                                              inputTypeInfo: TypeInformation[TIn],
                                                                                                              inputKeyTypeInfo: TypeInformation[TInKey],
                                                                                                              keyTypeInfo: TypeInformation[TKey],
                                                                                                              outputTypeInfo: TypeInformation[TOut])
  extends AggregateFunction[RecordWrapper[TIn, TInKey], util.Map[TKey, mutable.PriorityQueue[RecordWrapper[TIn, TInKey]]], RecordWrapper[TOut, Product]]
    with ResultTypeQueryable[RecordWrapper[TOut, Product]] {

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  /**
   * Gets the grouping key from the input record key.
   */
  protected def getKey(recordKey: TInKey): TKey

  protected def apply(values: Iterable[TIn]): TOut

  def getAccumulatorType: TypeInformation[util.Map[TKey, mutable.PriorityQueue[RecordWrapper[TIn, TInKey]]]] =
    new MapTypeInfo[TKey, mutable.PriorityQueue[RecordWrapper[TIn, TInKey]]](
      this.keyTypeInfo,
      new PriorityQueueTypeInformation[RecordWrapper[TIn, TInKey]](
        RecordWrapperTypeInformation.wrap(this.inputTypeInfo, this.inputKeyTypeInfo),
        new SequenceNumberOrdering[TIn, TInKey])
    )

  override def getProducedType: TypeInformation[RecordWrapper[TOut, Product]] =
    RecordWrapperTypeInformation.wrap[TOut](this.outputTypeInfo)

  override def createAccumulator(): util.Map[TKey, mutable.PriorityQueue[RecordWrapper[TIn, TInKey]]] =
    new util.HashMap[TKey, mutable.PriorityQueue[RecordWrapper[TIn, TInKey]]]()

  override def merge(acc: util.Map[TKey, mutable.PriorityQueue[RecordWrapper[TIn, TInKey]]],
                     acc1: util.Map[TKey, mutable.PriorityQueue[RecordWrapper[TIn, TInKey]]]): util.Map[TKey, mutable.PriorityQueue[RecordWrapper[TIn, TInKey]]] = {
    val merged = new util.HashMap[TKey, mutable.PriorityQueue[RecordWrapper[TIn, TInKey]]](acc)
    acc1.entrySet().asScala.foreach(entry => merged.merge(entry.getKey, entry.getValue, this.mergeQueues))
    merged
  }

  override def add(record: RecordWrapper[TIn, TInKey],
                   acc: util.Map[TKey, mutable.PriorityQueue[RecordWrapper[TIn, TInKey]]]): util.Map[TKey, mutable.PriorityQueue[RecordWrapper[TIn, TInKey]]] = {
    val key = this.getKey(record.key)
    acc.compute(key, this.addOrCreateQueue(record))
    acc
  }

  override def getResult(acc: util.Map[TKey, mutable.PriorityQueue[RecordWrapper[TIn, TInKey]]]): RecordWrapper[TOut, Product] = {
    val values = acc.values().asScala.flatMap(_.iterator).map(_.value)
    this.logger.info(s"__GETRESULT: ${values.toList}")
    val output = this.apply(values)
    RecordWrapper.wrap[TOut](output, 0L)
  }

  private def addOrCreateQueue(record: RecordWrapper[TIn, TInKey])
                              (key: TKey, items: mutable.PriorityQueue[RecordWrapper[TIn, TInKey]]): mutable.PriorityQueue[RecordWrapper[TIn, TInKey]] = {
    if (items == null) {
      val queue = new mutable.PriorityQueue[RecordWrapper[TIn, TInKey]]()(new SequenceNumberOrdering[TIn, TInKey])
      queue.enqueue(record)
      queue
    }
    else {
      items.enqueue(record)
      while (items.length > this.windowSize) {
        items.dequeue()
      }
      items
    }
  }

  private def mergeQueues(items1: mutable.PriorityQueue[RecordWrapper[TIn, TInKey]],
                          items2: mutable.PriorityQueue[RecordWrapper[TIn, TInKey]]): mutable.PriorityQueue[RecordWrapper[TIn, TInKey]] = {
    items1.enqueue(items2.toArray: _*)

    while (items1.length > this.windowSize) {
      items1.dequeue()
    }

    items1
  }
}
