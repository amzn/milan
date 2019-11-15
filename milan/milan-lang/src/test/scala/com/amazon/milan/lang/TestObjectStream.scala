package com.amazon.milan.lang

import com.amazon.milan.program._
import com.amazon.milan.test.IntRecord
import com.amazon.milan.typeutil._
import org.junit.Assert._
import org.junit.Test

import scala.language.existentials


object TestObjectStream {

  object StaticMapFunctions {
    def mapInputRecord(r: IntRecord) = IntRecord(r.i + 1)

    def getValue(r: IntRecord): Int = r.i

    def getValue2(r: IntRecord): Int = r.i
  }

  class Adder(val increment: Int) {
    def mapInputRecord(r: IntRecord) = IntRecord(r.i + increment)
  }

  def makeOutput(i: Int) = IntRecord(i + 1)
}

import com.amazon.milan.lang.TestObjectStream._


@Test
class TestObjectStream {
  @Test
  def test_ObjectStream_Map_WithObjectMapFromStaticClass_ReturnsObjectStreamWithExpectedNode(): Unit = {
    val input = Stream.of[IntRecord]
    val mapped = input.map(StaticMapFunctions.mapInputRecord(_))

    // Extract out the AST node for the mapped stream.
    val ComputedStream(_, _, MapRecord(source, FunctionDef(_, ApplyFunction(FunctionReference(mapFunctionTypeName, mapFunctionMethodName), _, _)))) = mapped.node
    val outputTypeName = classOf[IntRecord].getTypeName.replace("$", ".")
    assertEquals(input.node.getExpression, source)
    assertEquals("mapInputRecord", mapFunctionMethodName)
    assertEquals("com.amazon.milan.lang.TestObjectStream.StaticMapFunctions", mapFunctionTypeName)
  }

  @Test
  def test_ObjectStream_Map_WithObjectMapThatTakesFieldsAsArguments_ReturnsObjectStreamWithExpectedNode(): Unit = {
    val input = Stream.of[IntRecord]
    val mapped = input.map(r => TestObjectStream.makeOutput(r.i))

    val ComputedStream(_, _, mapExpression) = mapped.node
    val MapRecord(source, FunctionDef(_, ApplyFunction(_, args, _))) = mapExpression

    assertEquals(input.node.getExpression, source)

    val SelectField(SelectTerm(argName), fieldName) = args.head
    assertEquals("r", argName)
    assertEquals("i", fieldName)
  }

  @Test
  def test_ObjectStream_Map_WithTupleMapWithOneField_ReturnsTupleStreamWithExpectedNode(): Unit = {
    val input = Stream.of[IntRecord]
    val mapped = input.map(((r: IntRecord) => TestObjectStream.makeOutput(r.i)) as "x")

    val ComputedStream(_, _, MapFields(source, fields)) = mapped.node

    assertEquals(input.node.getExpression, source)
    assertEquals(1, fields.length)

    val fieldExpr = fields.head
    assertEquals("x", fieldExpr.fieldName)

    val FunctionDef(_, ApplyFunction(_, args, _)) = fieldExpr.expr
    val SelectField(SelectTerm(argName), fieldName) = args.head
    assertEquals("r", argName)
    assertEquals("i", fieldName)

    assertEquals(1, mapped.fields.length)
    assertEquals("x", mapped.fields.head.name)
    assertEquals(TypeUtil.getTypeName(classOf[IntRecord]), mapped.fields.head.fieldType.fullName)
  }

  @Test
  def test_ObjectStream_Map_WithTupleMapWithTwoFields_ReturnsTupleStreamWithExpectedNode(): Unit = {
    val input = Stream.of[IntRecord]
    val mapped = input.map(
      ((r: IntRecord) => TestObjectStream.makeOutput(r.i)) as "x",
      ((r: IntRecord) => TestObjectStream.makeOutput(r.i)) as "y")

    val ComputedStream(_, _, MapFields(source, fields)) = mapped.node

    assertEquals(input.node.getExpression, source)
    assertEquals(2, fields.length)

    val List(fieldExpr1, fieldExpr2) = fields
    assertEquals("x", fieldExpr1.fieldName)
    assertEquals("y", fieldExpr2.fieldName)

    val FunctionDef(_, ApplyFunction(_, args1, _)) = fieldExpr1.expr
    val SelectField(SelectTerm(argName1), fieldName1) = args1.head
    assertEquals("r", argName1)
    assertEquals("i", fieldName1)

    val FunctionDef(_, ApplyFunction(_, args2, _)) = fieldExpr2.expr
    val SelectField(SelectTerm(argName2), fieldName2) = args2.head
    assertEquals("r", argName2)
    assertEquals("i", fieldName2)

    assertEquals(2, mapped.fields.length)
    assertEquals("x", mapped.fields.head.name)
    assertEquals(TypeUtil.getTypeName(classOf[IntRecord]), mapped.fields.head.fieldType.fullName)
    assertEquals("y", mapped.fields(1).name)
    assertEquals(TypeUtil.getTypeName(classOf[IntRecord]), mapped.fields(1).fieldType.fullName)
  }

  @Test
  def test_ObjectStream_Where_WithPredicateTestingFieldEqualToOne_ReturnsObjectStreamWithExpectedNode(): Unit = {
    val input = Stream.of[IntRecord]
    val filtered = input.where(r => r.i == 1)

    val ComputedStream(_, _, Filter(source, predicate)) = filtered.node
    assertEquals(input.node.getExpression, source)

    // If this template extraction doesn't throw an exception then we got what we expected.
    val FunctionDef(_, Equals(SelectField(SelectTerm("r"), "i"), ConstantValue(1, _))) = predicate
  }

  @Test
  def test_ObjectStream_WithName_ReturnsCopyOfStreamWithNewNameAndOriginalStreamHasOriginalName(): Unit = {
    val original = Stream.of[IntRecord]
    val named = original.withName("foo")

    assertEquals("foo", named.streamName)
    assertNotEquals(original.streamName, named.streamName)
  }

  @Test
  def test_ObjectStream_ToField_ThenAddField_ThenJoin_CompilesWithoutError(): Unit = {
    val leftInput = Stream.of[IntRecord]
    val left =
      leftInput
        .toField("left")
        .addField(((t: Tuple1[IntRecord]) => t match {
          case Tuple1(r) => r.i
        }) as "left_i")

    val rightInput = Stream.of[IntRecord]
    val right =
      rightInput
        .toField("right")
        .addField(((t: Tuple1[IntRecord]) => t match {
          case Tuple1(r) => r.i
        }) as "right_i")

    left.leftJoin(right)
      .where((l, r) =>
        l != null && r != null &&
          (l match {
            case (_: IntRecord, left_i: Int) =>
              r match {
                case (_: IntRecord, right_i: Int) => left_i == right_i
              }
          }))
      .selectAll()
  }
}
