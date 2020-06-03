package com.amazon.milan.lang

import com.amazon.milan.program.{NamedField, _}
import com.amazon.milan.test.IntStringRecord
import com.amazon.milan.typeutil._
import org.junit.Assert._
import org.junit.Test

import scala.language.existentials


@Test
class TestTupleStream {
  @Test
  def test_TupleStream_Map_ToObjectStream_ReturnsStreamWithExpectedNode(): Unit = {
    val input = Stream.of[IntStringRecord]
    val tuple = input.map(r => fields(field("i", r.i), field("s", r.s)))
    val mapped = tuple.map(r => r match {
      case (i, s) => IntStringRecord(i, s)
    })

    val StreamMap(source, FunctionDef(params, Unpack(unpackParam, List("i", "s"), ApplyFunction(function, args, _)))) = mapped.expr

    assertEquals(tuple.expr, source)
    assertEquals("apply", function.functionName)
    assertEquals(2, args.length)

    // The target of the Unpack operation should be the first parameter.
    val SelectTerm(unpackTermName) = unpackParam
    assertEquals(params.head.name, unpackTermName)

    // Both function arguments should reference the unpacked values.
    val SelectTerm("i") = args.head
    val SelectTerm("s") = args.last
  }

  @Test
  def test_TupleStream_Map_ToTupleStream_ReturnsStreamWithExpectedExpression(): Unit = {
    val input = Stream.of[IntStringRecord]
    val tuple = input.map(r => fields(field("i", r.i), field("s", r.s)))
    val mapped = tuple.map(t => fields(
      field("i", t match { case (i, _) => i }),
      field("s", t match { case (_, s) => s })
    ))

    val StreamMap(source, FunctionDef(_, NamedFields(fieldList))) = mapped.expr
    assertEquals(tuple.expr, source)

    assertEquals("i", fieldList.head.fieldName)
    val Unpack(SelectTerm("t"), List("i", "_"), SelectTerm("i")) = fieldList.head.expr

    assertEquals("s", fieldList(1).fieldName)
    val Unpack(SelectTerm("t"), List("_", "s"), SelectTerm("s")) = fieldList(1).expr
  }

  @Test
  def test_TupleStream_Where_WithPredicateTestingFieldEqualToOne_ReturnsObjectStreamWithExpectedExpression(): Unit = {
    val input = Stream.of[IntStringRecord]
    val tuple = input.map(r => fields(field("i", r.i), field("s", r.s)))
    val filtered = tuple.where { case (i, s) => i == 1 }

    val Filter(source, predicate) = filtered.expr
    assertEquals(tuple.expr, source)

    // If this template extraction doesn't throw an exception then we got what we expected.
    val FunctionDef(_, Unpack(_, List("i", "s"), Equals(SelectTerm("i"), ConstantValue(1, _)))) = predicate
  }

  @Test
  def test_TupleStream_WithName_ReturnsCopyOfStreamWithNewNameAndOriginalStreamHasOriginalName(): Unit = {
    val original = Stream.of[IntStringRecord].map(r => fields(field("i", r.i), field("s", r.s)))
    val named = original.withName("foo")

    assertEquals("foo", named.streamName)
    assertNotEquals(original.streamName, named.streamName)
  }

  @Test
  def test_TupleStream_AddField_HasCorrectOutputTypeAndMapExpression(): Unit = {
    val tuple = Stream.of[IntStringRecord].map(r =>
      fields(
        field("i", r.i),
        field("s", r.s)))
    val output = tuple.addFields(r => fields(field("one", 1)))

    val expectedType = TypeDescriptor.createNamedTuple[(Int, String, Int)](List(("i", types.Int), ("s", types.String), ("one", types.Int)))
    assertEquals(expectedType, output.recordType)

    // If the extraction succeeds then the expression is correct.
    val StreamMap(_, FunctionDef(_, NamedFields(fieldsList))) = output.expr

    val NamedField("i", SelectField(SelectTerm("r"), "i")) = fieldsList.head
    val NamedField("s", SelectField(SelectTerm("r"), "s")) = fieldsList(1)
    val NamedField("one", ConstantValue(1, types.Int)) = fieldsList(2)
  }

  @Test
  def test_TupleStream_AddFields_WithTwoFields_HasCorrectOutputTypeAndMapExpression(): Unit = {
    val tuple = Stream.of[IntStringRecord].map(r =>
      fields(
        field("i", r.i),
        field("s", r.s)
      ))
    val output = tuple.addFields(r => fields(field("one", 1), field("two", 2L)))

    val expectedType = TypeDescriptor.createNamedTuple[(Int, String, Int)](List(("i", types.Int), ("s", types.String), ("one", types.Int), ("two", types.Long)))
    assertEquals(expectedType, output.recordType)

    // If the extraction succeeds then the expression is correct.
    val StreamMap(_, FunctionDef(_, NamedFields(fieldList))) = output.expr

    val NamedField("i", SelectField(SelectTerm("r"), "i")) = fieldList.head
    val NamedField("s", SelectField(SelectTerm("r"), "s")) = fieldList(1)
    val NamedField("one", ConstantValue(1, types.Int)) = fieldList(2)
    val NamedField("two", ConstantValue(2L, types.Long)) = fieldList(3)
  }

  @Test
  def test_TupleStream_AddFields_WithThreeFields_HasCorrectOutputTypeAndMapExpression(): Unit = {
    val tuple = Stream.of[IntStringRecord].map(r => fields(field("i", r.i), field("s", r.s)))
    val output = tuple.addFields(r => fields(field("one", 1), field("two", 2L), field("three", "3")))

    val expectedType = TypeDescriptor.createNamedTuple[(Int, String, Int)](List(
      ("i", types.Int),
      ("s", types.String),
      ("one", types.Int),
      ("two", types.Long),
      ("three", types.String)))
    assertEquals(expectedType, output.recordType)

    // If the extraction succeeds then the expression is correct.
    val StreamMap(_, FunctionDef(_, NamedFields(fieldList))) = output.expr

    val NamedField("i", SelectField(SelectTerm("r"), "i")) = fieldList.head
    val NamedField("s", SelectField(SelectTerm("r"), "s")) = fieldList(1)
    val NamedField("one", ConstantValue(1, types.Int)) = fieldList(2)
    val NamedField("two", ConstantValue(2L, types.Long)) = fieldList(3)
    val NamedField("three", ConstantValue("3", types.String)) = fieldList(4)
  }
}
