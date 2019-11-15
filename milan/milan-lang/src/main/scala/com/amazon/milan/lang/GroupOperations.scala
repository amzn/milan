package com.amazon.milan.lang

import com.amazon.milan.lang.internal.GroupedStreamMacros
import com.amazon.milan.types.Record

import scala.language.experimental.macros


/**
 * Standard operations that apply to all record groupings.
 *
 * @tparam T       The type of stream records.
 * @tparam TKey    The type of the group key.
 * @tparam TStream The type of the stream object that was used to create the grouping.
 */
class GroupOperations[T, TKey, TStream <: Stream[T, _]] {
  def select[TOut <: Record](f: (TKey, T) => TOut): ObjectStream[TOut] = macro GroupedStreamMacros.selectObject[T, TKey, TOut]

  def select[TF](f: Function2FieldStatement[TKey, T, TF]): TupleStream[Tuple1[TF]] = macro GroupedStreamMacros.selectTuple1[T, TKey, TF]

  def select[T1, T2](f1: Function2FieldStatement[TKey, T, T1],
                     f2: Function2FieldStatement[TKey, T, T2]): TupleStream[(T1, T2)] = macro GroupedStreamMacros.selectTuple2[T, TKey, T1, T2]

  def select[T1, T2, T3](f1: Function2FieldStatement[TKey, T, T1],
                         f2: Function2FieldStatement[TKey, T, T2],
                         f3: Function2FieldStatement[TKey, T, T3]): TupleStream[(T1, T2, T3)] = macro GroupedStreamMacros.selectTuple3[T, TKey, T1, T2, T3]

  def maxBy[TArg: Ordering](f: T => TArg): TStream = macro GroupedStreamMacros.maxBy[T, TArg, TStream]
}
