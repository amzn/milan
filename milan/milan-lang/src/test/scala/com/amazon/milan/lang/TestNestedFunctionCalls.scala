package com.amazon.milan.lang

import com.amazon.milan.program.{FunctionDef, MapRecord, SelectTerm}
import com.amazon.milan.test.{IntKeyValueRecord, StringRecord}
import org.junit.Test


object TestNestedFunctionCalls {
  def combineKeyValue(key: Int, value: Int): String = s"$key.$value"
}

import com.amazon.milan.lang.TestNestedFunctionCalls._


@Test
class TestNestedFunctionCalls {

  @Test
  def test_NestedFunctionCalls_WithGroupByThenMapUsingUserFunction_PutsUsersFunctionExpressionInTree(): Unit = {
    def mapStream(stream: Stream[IntKeyValueRecord]): Stream[StringRecord] = {
      stream.map(r => StringRecord(combineKeyValue(r.key, r.value)))
    }

    val input = Stream.of[IntKeyValueRecord]
    val grouped = input.groupBy(r => r.key)
    val mapped = grouped.map((key, group) => mapStream(group))

    val MapRecord(_, FunctionDef(List("key", "group"), MapRecord(SelectTerm("group"), _))) = mapped.expr
  }
}
