package com.amazon.milan.lang

import com.amazon.milan.program.{FunctionDef, SelectTerm, StreamMap, ValueDef}
import com.amazon.milan.test.{IntKeyValueRecord, StringRecord}
import org.junit.Test


object TestFunctionOfStream {
  def combineKeyValue(key: Int, value: Int): String = s"$key.$value"
}

import com.amazon.milan.lang.TestFunctionOfStream._


@Test
class TestFunctionOfStream {
  @Test
  def test_FunctionOfStream_WithGroupByThenMapUsingUserFunction_PutsUsersFunctionExpressionInTree(): Unit = {
    def mapStream(stream: Stream[IntKeyValueRecord]): Stream[StringRecord] = {
      stream.map(r => StringRecord(combineKeyValue(r.key, r.value)))
    }

    val input = Stream.of[IntKeyValueRecord]
    val grouped = input.groupBy(r => r.key)
    val mapped = grouped.map((key, group) => mapStream(group))

    val StreamMap(_, FunctionDef(List(ValueDef("key", _), ValueDef("group", _)), StreamMap(SelectTerm("group"), _))) = mapped.expr
  }
}
