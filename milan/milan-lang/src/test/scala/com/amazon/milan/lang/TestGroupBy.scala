package com.amazon.milan.lang

import com.amazon.milan.lang.aggregation._
import com.amazon.milan.program._
import com.amazon.milan.test.{IntKeyValueRecord, IntRecord}
import com.amazon.milan.typeutil.{FieldDescriptor, types}
import org.junit.Assert._
import org.junit.Test

object TestGroupBy {
  def transformGroup(group: Stream[IntKeyValueRecord]): Stream[IntRecord] = {
    group.map(r => IntRecord(r.value))
  }
}

import com.amazon.milan.lang.TestGroupBy._


@Test
class TestGroupBy {
  @Test
  def test_Stream_GroupBy_ReturnsStreamWithCorrectInputNodeAndKeyFunction(): Unit = {
    val stream = Stream.of[IntKeyValueRecord]
    val grouped = stream.groupBy(r => r.key)

    // If this extraction statement doesn't crash then we're good.
    val GroupBy(_, FunctionDef(List("r"), SelectField(SelectTerm("r"), "key"))) = grouped.expr

    assertEquals(stream.expr, grouped.expr.getChildren.head)
  }

  @Test
  def test_Stream_GroupBy_ThenSelectToTuple_ReturnsStreamWithCorrectFieldComputationExpression(): Unit = {
    val stream = Stream.of[IntKeyValueRecord]
    val grouped = stream.groupBy(r => r.key)
    val selected = grouped.select(((key: Int, r: IntKeyValueRecord) => key) as "i")

    assertEquals(1, selected.recordType.fields.length)
    assertEquals(FieldDescriptor("i", types.Int), selected.recordType.fields.head)

    val map = selected.expr.asInstanceOf[MapFields]
    assertEquals(1, map.fields.length)
    assertEquals("i", map.fields.head.fieldName)

    // If this extraction statement doesn't crash then we're good.
    val FunctionDef(List("key", "r"), SelectTerm("key")) = map.fields.head.expr
  }

  @Test
  def test_Stream_GroupBy_ThenSelectToObject_ReturnsStreamWithCorrectMapFunction(): Unit = {
    val stream = Stream.of[IntKeyValueRecord]
    val grouped = stream.groupBy(r => r.key)
    val selected = grouped.select((key, r) => argmax(r.value, r))

    val map = selected.expr.asInstanceOf[MapRecord]

    // If this extraction statement doesn't crash then we're good.
    val FunctionDef(List("key", "r"), ArgMax(Tuple(List(SelectField(SelectTerm("r"), "value"), SelectTerm("r"))))) = map.expr
  }

  @Test
  def test_Stream_GroupBy_ThenMaxBy_ReturnsObjectStreamWithCorrectMapFunction(): Unit = {
    val input = Stream.of[IntKeyValueRecord]
    val output = input.groupBy(r => r.key).maxBy(r => r.value)

    val MapRecord(_, mapFunctionDef) = output.expr
    val FunctionDef(List(_, "r"), ArgMax(Tuple(List(SelectField(SelectTerm("r"), "value"), SelectTerm("r"))))) = mapFunctionDef
  }

  @Test
  def test_Stream_GroupBy_ThenMap_(): Unit = {
    //TODO: finish this.
    val input = Stream.of[IntKeyValueRecord].withName("input")
    val grouped = input.groupBy(r => r.key)
    val mapped = grouped.map((key, group) => transformGroup(group))
  }
}
