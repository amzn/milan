package com.amazon.milan.lang

import java.time.Duration

import com.amazon.milan.lang.aggregation._
import com.amazon.milan.program
import com.amazon.milan.program.{GroupBy, _}
import com.amazon.milan.test.{DateIntRecord, DateKeyValueRecord}
import com.amazon.milan.typeutil.{FieldDescriptor, types}
import org.junit.Assert._
import org.junit.Test

import scala.language.existentials


@Test
class TestWindow {
  @Test
  def test_ObjectStream_TumblingWindow_ReturnsStreamWithCorrectInputNodeAndWindowProperties(): Unit = {
    val stream = Stream.of[DateIntRecord]
    val windowed = stream.tumblingWindow(r => r.dateTime, Duration.ofHours(1), Duration.ofMinutes(30))

    val TumblingWindow(_, dateExtractorFunc, period, offset) = windowed.expr

    // If this extraction doesn't throw an exception then the formula is correct.
    val FunctionDef(List(ValueDef("r", _)), SelectField(SelectTerm("r"), "dateTime")) = dateExtractorFunc

    assertEquals(Duration.ofHours(1), period.asJava)
    assertEquals(Duration.ofMinutes(30), offset.asJava)
  }

  @Test
  def test_ObjectStream_TumblingWindow_ThenSelectToTuple_ReturnsStreamWithCorrectFieldComputationExpression(): Unit = {
    val stream = Stream.of[DateIntRecord]
    val grouped = stream.tumblingWindow(r => r.dateTime, Duration.ofHours(1), Duration.ofMinutes(30))
    val selected = grouped.select((key, r) => fields(field("max", max(r.i))))

    val StreamMap(source, FunctionDef(_, NamedFields(fieldList))) = selected.expr

    assertEquals(1, selected.recordType.fields.length)
    assertEquals(FieldDescriptor("max", types.Int), selected.recordType.fields.head)

    assertEquals(1, fieldList.length)
    assertEquals("max", fieldList.head.fieldName)

    // If this extraction statement doesn't crash then we're good.
    val Max(SelectField(SelectTerm("r"), "i")) = fieldList.head.expr
  }

  @Test
  def test_ObjectStream_SlidingWindow_ReturnsStreamWithCorrectInputNodeAndWindowProperties(): Unit = {
    val stream = Stream.of[DateIntRecord]
    val windowed = stream.slidingWindow(r => r.dateTime, Duration.ofHours(1), Duration.ofMinutes(10), Duration.ofMinutes(30))

    val SlidingWindow(_, dateExtractorFunc, size, slide, offset) = windowed.expr

    val FunctionDef(List(ValueDef("r", _)), SelectField(SelectTerm("r"), "dateTime")) = dateExtractorFunc

    assertEquals(Duration.ofHours(1), size.asJava)
    assertEquals(Duration.ofMinutes(10), slide.asJava)
    assertEquals(Duration.ofMinutes(30), offset.asJava)
  }

  @Test
  def test_ObjectStream_GroupBy_ThenTumblingWindow_ReturnsStreamWithCorrectInputNodeAndWindowProperties(): Unit = {
    val input = Stream.of[DateKeyValueRecord].withId("input")
    val output = input.groupBy(r => r.key)
      .tumblingWindow(r => r.dateTime, Duration.ofMinutes(5), Duration.ZERO)
      .select((windowStart, r) => any(r))

    val StreamMap(windowExpr, FunctionDef(List(ValueDef("windowStart", _), ValueDef("r", _)), First(SelectTerm("r")))) = output.expr
    val TumblingWindow(groupExpr, FunctionDef(List(ValueDef("r", _)), SelectField(SelectTerm("r"), "dateTime")), program.Duration(300000), program.Duration(0)) = windowExpr
    val GroupBy(ExternalStream("input", "input", _), FunctionDef(List(ValueDef("r", _)), SelectField(SelectTerm("r"), "key"))) = groupExpr
  }
}
