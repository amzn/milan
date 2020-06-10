package com.amazon.milan

import scala.language.implicitConversions


package object stream {
  /**
   * Standard unfold operation, where at every step a state variable is evolved and an output item is produced.
   *
   * @param start The initial state.
   * @param f     A function that takes the current state and returns the new state and the item to output.
   *              The unfold operation continues until this function returns None.
   * @tparam A The state type.
   * @tparam B The output type.
   * @return A [[Stream]] of the output items.
   */
  def unfold[A, B](start: A)(f: A => Option[(A, B)]): Stream[B] =
    f(start).map { case (a, b) => b #:: unfold(a)(f) }.getOrElse(scala.Stream.empty)

  /**
   * Unfold operation where the state type is the same as the output type.
   * At every step, the new state is sent to the output.
   *
   * @param start The initial state. It will also be the first item in the output stream.
   * @param f     A function that takes the current state and returns a new state, which is also place on the output stream.
   *              The unfold operation continues until this function returns None.
   * @tparam A The state and output type.
   * @return A [[Stream]] of the output items.
   */
  def unfoldSingle[A](start: A)(f: A => Option[A]): Stream[A] =
    start #:: f(start).map(a => unfoldSingle(a)(f)).getOrElse(scala.Stream.empty)
}
