package com.amazon.milan.compiler.scala.event.operators

import com.amazon.milan.compiler.scala.event.{KeyedStateInterface, RecordWrapper}


/**
 * Base class for apply operations on sliding windows over grouped streams.
 *
 * @param windowSize The number of records per group in the window.
 * @tparam TIn       The input record type.
 * @tparam TFullKey  The full key type of the record.
 * @tparam TStateKey The type of the key to the state interface. Usually this will be the same as the context key,
 *                   unless the context key is empty in which case this will be filled in with a placeholder type.
 * @tparam TKey      The key type of the record in the context of this operation..
 * @tparam TOut      The output record type.
 * @tparam TOutKey   The output key type.
 */
abstract class KeyedSlidingRecordWindowApply[TIn, TFullKey, TStateKey, TKey, TOut, TOutKey](windowSize: Int) {
  protected val windowState: KeyedStateInterface[TStateKey, Map[TKey, List[TIn]]]

  protected def applyWindow(items: Iterable[TIn], key: TKey): TOut

  protected def getStateKey(fullKey: TFullKey): TStateKey

  protected def getLocalKey(fullKey: TFullKey): TKey

  protected def getOutputKey(fullKey: TFullKey): TOutKey

  def processRecord(record: RecordWrapper[TIn, TFullKey]): RecordWrapper[TOut, TOutKey] = {
    val stateKey = this.getStateKey(record.key)

    val window =
      windowState.getState(stateKey) match {
        case Some(w) => w
        case None => Map.empty[TKey, List[TIn]]
      }

    // Get the window contents for this key, and update them so that we have at most windowSize items, evicting the
    // oldest item if necessary.
    val key = this.getLocalKey(record.key)
    val keyList = window.getOrElse(key, List.empty)
    val newKeyList = record.value :: keyList.take(this.windowSize - 1)

    val newWindow = window + Tuple2(key, newKeyList)
    this.windowState.setState(stateKey, newWindow)

    val windowRecords = newWindow.values.flatten

    // Passing the key here doesn't really make sense, because this applies over all keys.
    // But for now, the ScanOperation interface requires it, which means we need to supply one.
    val outputValue = this.applyWindow(windowRecords, key)
    val outputKey = this.getOutputKey(record.key)
    RecordWrapper(outputValue, outputKey)
  }
}
