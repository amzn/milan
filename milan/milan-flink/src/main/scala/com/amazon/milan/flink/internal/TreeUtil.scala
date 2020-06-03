package com.amazon.milan.flink.internal

import com.amazon.milan.program
import com.amazon.milan.program.{FunctionDef, SelectTerm, Unpack}


object TreeUtil {
  /**
   * Given a function, creates a new function that returns a tuple of the output of the original function and the input
   * to the function.
   */
  def zipWithInput(functionDef: FunctionDef): FunctionDef = {
    if (functionDef.arguments.length != 1) {
      throw new IllegalArgumentException(s"Only functions of a single argument are supported.")
    }

    val argName = functionDef.arguments.head.name

    FunctionDef(functionDef.arguments, program.Tuple(List(functionDef.body, SelectTerm(argName))))
  }

  /**
   * Gets a function that extracts a single element from a tuple.
   *
   * @param tupleSize    The number of elements in the tuple.
   * @param elementIndex The zero-based index of the element to extract.
   */
  def getTupleElementFunction(tupleSize: Int, elementIndex: Int): FunctionDef = {
    val elementNames = List.tabulate(tupleSize)(i => s"x$i")
    FunctionDef.create(List("t"), Unpack(SelectTerm("t"), elementNames, SelectTerm(elementNames(elementIndex))))
  }
}
