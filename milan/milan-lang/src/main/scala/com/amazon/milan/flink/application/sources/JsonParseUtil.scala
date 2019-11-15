package com.amazon.milan.flink.application.sources

import com.amazon.milan.flink.RuntimeEvaluator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext


trait ClassOfArrayToListFunctionGenerator {
  def apply[T]: Function[Class[Array[T]], List[T]]
}


object JsonParseUtil {
  def parseList(elementTypeName: String, parser: JsonParser, context: DeserializationContext): List[_] = {
    val readListGenerator = new ClassOfArrayToListFunctionGenerator {
      override def apply[T]: Function[Class[Array[T]], List[T]] =
        (listClass: Class[Array[T]]) => context.readValue[Array[T]](parser, listClass).toList
    }

    val eval = RuntimeEvaluator.instance

    eval.evalFunction[ClassOfArrayToListFunctionGenerator, List[_]](
      "functionGenerator",
      "com.amazon.milan.flink.application.sources.ClassOfArrayToListFunctionGenerator",
      s"functionGenerator[$elementTypeName](classOf[Array[$elementTypeName]])",
      readListGenerator)
  }
}
