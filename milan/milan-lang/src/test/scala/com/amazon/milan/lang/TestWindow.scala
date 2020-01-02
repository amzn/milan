package com.amazon.milan.lang

import java.time.{Duration, Instant}

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

    val ComputedGraphNode(_, TumblingWindow(_, dateExtractorFunc, period, offset)) = windowed.node

    // If this extraction doesn't throw an exception then the formula is correct.
    val FunctionDef(List("r"), SelectField(SelectTerm("r"), "dateTime")) = dateExtractorFunc

    assertEquals(Duration.ofHours(1), period.asJava)
    assertEquals(Duration.ofMinutes(30), offset.asJava)
  }

  @Test
  def test_ObjectStream_TumblingWindow_ThenSelectToTuple_ReturnsStreamWithCorrectFieldComputationExpression(): Unit = {
    val stream = Stream.of[DateIntRecord]
    val grouped = stream.tumblingWindow(r => r.dateTime, Duration.ofHours(1), Duration.ofMinutes(30))
    val selected = grouped.select(((key: Instant, r: DateIntRecord) => max(r.i)) as "max")

    val ComputedStream(_, _, MapFields(source, fields)) = selected.node

    assertEquals(1, selected.recordType.fields.length)
    assertEquals(FieldDescriptor("max", types.Int), selected.recordType.fields.head)

    assertEquals(1, fields.length)
    assertEquals("max", fields.head.fieldName)

    // If this extraction statement doesn't crash then we're good.
    val FunctionDef(List("key", "r"), Max(SelectField(SelectTerm("r"), "i"))) = fields.head.expr
  }

  @Test
  def test_ObjectStream_SlidingWindow_ReturnsStreamWithCorrectInputNodeAndWindowProperties(): Unit = {
    val stream = Stream.of[DateIntRecord]
    val windowed = stream.slidingWindow(r => r.dateTime, Duration.ofHours(1), Duration.ofMinutes(10), Duration.ofMinutes(30))

    val ComputedGraphNode(_, SlidingWindow(_, dateExtractorFunc, size, slide, offset)) = windowed.node

    val FunctionDef(List("r"), SelectField(SelectTerm("r"), "dateTime")) = dateExtractorFunc

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

    val MapRecord(windowExpr, FunctionDef(List("windowStart", "r"), First(SelectTerm("r")))) = output.node.getExpression
    val TumblingWindow(groupExpr, FunctionDef(List("r"), SelectField(SelectTerm("r"), "dateTime")), program.Duration(300000), program.Duration(0)) = windowExpr
    val GroupBy(Ref("input"), FunctionDef(List("r"), SelectField(SelectTerm("r"), "key"))) = groupExpr
  }
}
