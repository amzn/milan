package com.amazon.milan.lang

import com.amazon.milan.lang.aggregation._
import com.amazon.milan.program.{ValueDef, _}
import com.amazon.milan.test.{IntKeyValueRecord, IntRecord}
import com.amazon.milan.typeutil.{FieldDescriptor, types}
import org.junit.Assert._
import org.junit.Test

object TestGroupBy {
  def transformGroup(group: Stream[IntKeyValueRecord]): Stream[IntRecord] = {
    group.map(r => IntRecord(r.value))
  }
}


@Test
class TestGroupBy {
  @Test
  def test_Stream_GroupBy_ReturnsStreamWithCorrectInputNodeAndKeyFunction(): Unit = {
    val stream = Stream.of[IntKeyValueRecord]
    val grouped = stream.groupBy(r => r.key)

    // If this extraction statement doesn't crash then we're good.
    val GroupBy(_, FunctionDef(List(ValueDef("r", _)), SelectField(SelectTerm("r"), "key"))) = grouped.expr

    assertEquals(stream.expr, grouped.expr.getChildren.head)
  }

  @Test
  def test_Stream_GroupBy_ThenSelectToTuple_ReturnsStreamWithCorrectFieldComputationExpression(): Unit = {
    val stream = Stream.of[IntKeyValueRecord]
    val grouped = stream.groupBy(r => r.key)
    val selected = grouped.select((key, r) => fields(field("i", key)))

    assertEquals(1, selected.recordType.fields.length)
    assertEquals(FieldDescriptor("i", types.Int), selected.recordType.fields.head)

    val aggExpr = selected.expr.asInstanceOf[Aggregate]
    assertEquals(1, aggExpr.recordType.fields.length)
    assertEquals("i", aggExpr.recordType.fields.head.name)

    // If this extraction statement doesn't crash then we're good.
    val FunctionDef(List(ValueDef("key", _), ValueDef("r", _)), NamedFields(List(NamedField("i", SelectTerm("key"))))) = aggExpr.expr
  }

  @Test
  def test_Stream_GroupBy_ThenSelectToObject_ReturnsStreamWithCorrectMapFunction(): Unit = {
    val stream = Stream.of[IntKeyValueRecord]
    val grouped = stream.groupBy(r => r.key)
    val selected = grouped.select((key, r) => argmax(r.value, r))

    val aggExpr = selected.expr.asInstanceOf[Aggregate]

    // If this extraction statement doesn't crash then we're good.
    val FunctionDef(List(ValueDef("key", _), ValueDef("r", _)), ArgMax(Tuple(List(SelectField(SelectTerm("r"), "value"), SelectTerm("r"))))) = aggExpr.expr
  }
}
