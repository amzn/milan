package com.amazon.milan.lang

import com.amazon.milan.lang.internal.GroupedStreamMacros

import scala.language.experimental.macros


/**
 * Standard operations that apply to all record groupings.
 *
 * @tparam T The type of stream records.
 */
trait UnkeyedGroupOperations[T] {
  /**
   * Perform an aggregate computation on the records in each group.
   * select() allows you to write an expression in the style of a SQL SELECT statement in order to perform an
   * aggregate computation.
   * The alternative is to use flatMap and perform the computation using standard stream operations.
   *
   * @param f The aggregate function to apply.
   * @tparam TOut The function output type.
   * @return A [[Stream]] representing the output records from all groups.
   */
  def select[TOut](f: T => TOut): Stream[TOut] = macro GroupedStreamMacros.unkeyedSelectObject[T, TOut]

  /**
   * Maps each stream of grouped records to another stream, and combines all output streams into a single stream.
   *
   * @param f A function that maps each stream of grouped records to another stream.
   * @tparam TOut The output record type.
   * @return A [[Stream]] containing all of the output records from all groups.
   */
  def flatMap[TOut](f: Stream[T] => Stream[TOut]): Stream[TOut] = macro GroupedStreamMacros.unkeyedFlatMap[T, TOut]
}


/**
 * Standard operations that apply to all record groupings where each group has an associated key.
 *
 * @tparam T    The type of stream records.
 * @tparam TKey The type of the group key.
 */
trait KeyedGroupOperations[T, TKey] {
  /**
   * Perform an aggregate computation on the records in each group.
   * select() allows you to write an expression in the style of a SQL SELECT statement in order to perform an
   * aggregate computation.
   * The alternative is to use flatMap and perform the computation using standard stream operations.
   *
   * @param f The aggregate function to apply.
   * @tparam TOut The function output type.
   * @return A [[Stream]] representing the output records from all groups.
   */
  def select[TOut](f: (TKey, T) => TOut): Stream[TOut] = macro GroupedStreamMacros.keyedSelectObject[T, TKey, TOut]

  /**
   * Maps each stream of grouped records to another stream, and combines all output streams into a single stream.
   *
   * @param f A function that maps each stream of grouped records to another stream.
   * @tparam TOut The output record type.
   * @return A [[Stream]] containing all of the output records from all groups.
   */
  def flatMap[TOut](f: (TKey, Stream[T]) => Stream[TOut]): Stream[TOut] = macro GroupedStreamMacros.keyedFlatMap[T, TKey, TOut]
}
