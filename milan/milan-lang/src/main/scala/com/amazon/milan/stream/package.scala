package com.amazon.milan

import scala.language.implicitConversions


package object stream {
  def unfold[A, B](start: A)(f: A => Option[(A, B)]): Stream[B] =
    f(start).map { case (a, b) => b #:: unfold(a)(f) }.getOrElse(scala.Stream.empty)

  def unfoldSingle[A](start: A)(f: A => Option[A]): Stream[A] =
    start #:: f(start).map(a => unfoldSingle(a)(f)).getOrElse(scala.Stream.empty)
}
