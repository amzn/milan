package com.amazon.milan.lang

import com.amazon.milan.program._
import com.amazon.milan.test.IntStringRecord
import com.amazon.milan.types.RecordIdFieldName
import com.amazon.milan.typeutil._
import org.junit.Assert._
import org.junit.Test

import scala.language.existentials


@Test
class TestTupleStream {
  @Test
  def test_TupleStream_Map_ToObjectStream_ReturnsStreamWithExpectedNode(): Unit = {
    val input = Stream.of[IntStringRecord]
    val tuple = input.map(
      ((r: IntStringRecord) => r.i) as "i",
      ((r: IntStringRecord) => r.s) as "s")
    val mapped = tuple.map { case (i, s) => IntStringRecord(i, s) }

    val ComputedStream(_, _, mapFunction) = mapped.node
    val MapRecord(source, FunctionDef(params, Unpack(unpackParam, List("i", "s"), ApplyFunction(function, args, _)))) = mapFunction

    assertEquals(tuple.node.getExpression, source)
    assertEquals("apply", function.functionName)
    assertEquals(2, args.length)
    assertEquals(unpackParam.termName, params.head)

    // Both function arguments should reference the unpacked values.
    val SelectTerm("i") = args.head
    val SelectTerm("s") = args.last
  }

  @Test
  def test_TupleStream_Map_ToTupleStream_ReturnsStreamWithExpectedNode(): Unit = {
    val input = Stream.of[IntStringRecord]
    val tuple = input.map(
      ((r: IntStringRecord) => r.i) as "i",
      ((r: IntStringRecord) => r.s) as "s")
    val mapped = tuple.map(
      ((t: (Int, String)) => t match {
        case (i, _) => i
      }) as "i",
      ((t: (Int, String)) => t match {
        case (_, s) => s
      }) as "s")

    val ComputedStream(_, _, MapFields(source, fields)) = mapped.node
    assertEquals(tuple.node.getExpression, source)

    assertEquals("i", fields.head.fieldName)
    val FunctionDef(List("t"), Unpack(SelectTerm("t"), List("i", "_"), SelectTerm("i"))) = fields.head.expr

    assertEquals("s", fields(1).fieldName)
    val FunctionDef(List("t"), Unpack(SelectTerm("t"), List("_", "s"), SelectTerm("s"))) = fields(1).expr
  }

  @Test
  def test_TupleStream_Where_WithPredicateTestingFieldEqualToOne_ReturnsObjectStreamWithExpectedNode(): Unit = {
    val input = Stream.of[IntStringRecord]
    val tuple = input.map(
      ((r: IntStringRecord) => r.i) as "i",
      ((r: IntStringRecord) => r.s) as "s")
    val filtered = tuple.where { case (i, s) => i == 1 }

    val ComputedStream(_, _, Filter(source, predicate)) = filtered.node
    assertEquals(tuple.node.getExpression, source)

    // If this template extraction doesn't throw an exception then we got what we expected.
    val FunctionDef(_, Unpack(_, List("i", "s"), Equals(SelectTerm("i"), ConstantValue(1, _)))) = predicate
  }

  @Test
  def test_TupleStream_WithName_ReturnsCopyOfStreamWithNewNameAndOriginalStreamHasOriginalName(): Unit = {
    val original = Stream.of[IntStringRecord].map(
      ((r: IntStringRecord) => r.i) as "i",
      ((r: IntStringRecord) => r.s) as "s")
    val named = original.withName("foo")

    assertEquals("foo", named.streamName)
    assertNotEquals(original.streamName, named.streamName)
  }

  @Test
  def test_TupleStream_ProjectOnto_WithCorrectConstructor_DoesNotThrow(): Unit = {
    val tuple = Stream.of[IntStringRecord].map(
      ((r: IntStringRecord) => r.i) as "i",
      ((r: IntStringRecord) => r.s) as "s")
    tuple.projectOnto[IntStringRecord]
  }

  @Test(expected = classOf[InvalidProgramException])
  def test_TupleStream_ProjectOnto_WithoutCorrectConstructor_ThrowsInvalidProjectException(): Unit = {
    val tuple = Stream.of[IntStringRecord].map(
      ((r: IntStringRecord) => r.i) as "i",
      ((r: IntStringRecord) => r.s) as "j")
    tuple.projectOnto[IntStringRecord]
  }

  @Test
  def test_TupleStream_ProjectOnto_CreatesExpectedMapFunctionDef(): Unit = {
    val tuple = Stream.of[IntStringRecord].map(
      ((r: IntStringRecord) => r.i) as "i",
      ((r: IntStringRecord) => r.s) as "s")
    val projected = tuple.projectOnto[IntStringRecord]

    // If these extractions succeed then everything checks out.
    val ComputedStream(_, _, MapRecord(_, mapExpr)) = projected.node
    val FunctionDef(List("r"), CreateInstance(_, args)) = mapExpr
    val List(SelectField(SelectTerm("r"), RecordIdFieldName), SelectField(SelectTerm("r"), "i"), SelectField(SelectTerm("r"), "s")) = args
  }

  @Test
  def test_TupleStream_AddField_HasCorrectOutputTypeAndMapExpression(): Unit = {
    val tuple = Stream.of[IntStringRecord].map(
      ((r: IntStringRecord) => r.i) as "i",
      ((r: IntStringRecord) => r.s) as "s")
    val output = tuple.addField[Int](((_: (Int, String)) => 1) as "one")

    val expectedType = TypeDescriptor.createNamedTuple[(Int, String, Int)](List(("i", types.Int), ("s", types.String), ("one", types.Int)))
    assertEquals(expectedType, output.getRecordType)

    // If the extraction succeeds then the expression is correct.
    val MapFields(_, List(
    FieldDefinition("i", FunctionDef(List("r"), SelectField(SelectTerm("r"), "i"))),
    FieldDefinition("s", FunctionDef(List("r"), SelectField(SelectTerm("r"), "s"))),
    FieldDefinition("one", FunctionDef(_, ConstantValue(1, types.Int))))) = output.node.getExpression
  }

  @Test
  def test_TupleStream_AddFields_WithTwoFields_HasCorrectOutputTypeAndMapExpression(): Unit = {
    val tuple = Stream.of[IntStringRecord].map(
      ((r: IntStringRecord) => r.i) as "i",
      ((r: IntStringRecord) => r.s) as "s")
    val output = tuple.addFields(((_: (Int, String)) => 1) as "one", ((_: (Int, String)) => 2L) as "two")

    val expectedType = TypeDescriptor.createNamedTuple[(Int, String, Int)](List(("i", types.Int), ("s", types.String), ("one", types.Int), ("two", types.Long)))
    assertEquals(expectedType, output.getRecordType)

    // If the extraction succeeds then the expression is correct.
    val MapFields(_, List(
    FieldDefinition("i", FunctionDef(List("r"), SelectField(SelectTerm("r"), "i"))),
    FieldDefinition("s", FunctionDef(List("r"), SelectField(SelectTerm("r"), "s"))),
    FieldDefinition("one", FunctionDef(_, ConstantValue(1, types.Int))),
    FieldDefinition("two", FunctionDef(_, ConstantValue(2L, types.Long))))) = output.node.getExpression
  }

  @Test
  def test_TupleStream_AddFields_WithThreeFields_HasCorrectOutputTypeAndMapExpression(): Unit = {
    val tuple = Stream.of[IntStringRecord].map(
      ((r: IntStringRecord) => r.i) as "i",
      ((r: IntStringRecord) => r.s) as "s")
    val output = tuple.addFields(
      ((_: (Int, String)) => 1) as "one",
      ((_: (Int, String)) => 2L) as "two",
      ((_: (Int, String)) => "3") as "three")

    val expectedType = TypeDescriptor.createNamedTuple[(Int, String, Int)](List(
      ("i", types.Int),
      ("s", types.String),
      ("one", types.Int),
      ("two", types.Long),
      ("three", types.String)))
    assertEquals(expectedType, output.getRecordType)

    // If the extraction succeeds then the expression is correct.
    val MapFields(_, List(
    FieldDefinition("i", FunctionDef(List("r"), SelectField(SelectTerm("r"), "i"))),
    FieldDefinition("s", FunctionDef(List("r"), SelectField(SelectTerm("r"), "s"))),
    FieldDefinition("one", FunctionDef(_, ConstantValue(1, types.Int))),
    FieldDefinition("two", FunctionDef(_, ConstantValue(2L, types.Long))),
    FieldDefinition("three", FunctionDef(_, ConstantValue("3", types.String))))) = output.node.getExpression
  }
}
