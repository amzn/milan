package com.amazon.milan.compiler.scala

import com.amazon.milan.lang


object StreamAppTester {
  def compile[TIn, TOut](inputStream: lang.Stream[TIn], outputStream: lang.Stream[TOut]): Stream[TIn] => Stream[TOut] = {
    val functionDef = ScalaStreamGenerator.generateAnonymousFunction(List(inputStream), outputStream)
    RuntimeEvaluator.instance.eval[Stream[TIn] => Stream[TOut]](functionDef)
  }
}
