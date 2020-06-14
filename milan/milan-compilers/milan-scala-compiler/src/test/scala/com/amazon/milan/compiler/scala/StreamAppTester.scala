package com.amazon.milan.compiler.scala

import com.amazon.milan.lang
import com.amazon.milan.lang.StreamGraph


object StreamAppTester {
  def compile[TIn, TOut](outputStream: lang.Stream[TOut]): Stream[TIn] => Stream[TOut] = {
    val graph = new StreamGraph(outputStream)

    val functionDef = ScalaStreamGenerator.generateAnonymousFunction(graph, outputStream)
    RuntimeEvaluator.instance.eval[Stream[TIn] => Stream[TOut]](functionDef)
  }
}
