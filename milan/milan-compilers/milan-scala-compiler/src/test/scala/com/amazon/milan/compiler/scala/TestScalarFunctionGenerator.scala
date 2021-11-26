package com.amazon.milan.compiler.scala

import com.amazon.milan.program.{ApplyFunction, CreateInstance, FunctionDef, FunctionReference, SelectField, SelectTerm, TypeChecker, Unpack, ValueDef}
import com.amazon.milan.typeutil.{TypeDescriptor, createTypeDescriptor, types}
import org.junit.Assert.assertEquals
import org.junit.Test


object TestScalarFunctionGenerator {

  case class IntRecord(i: Int)

}

import com.amazon.milan.compiler.scala.TestScalarFunctionGenerator._


@Test
class TestScalarFunctionGenerator {
  @Test
  def test_ScalarFunctionGenerator_WithTwoObjectInputs_ReturnsExpectedCode(): Unit = {
    val input1 = TypeDescriptor.of[IntRecord]
    val input2 = TypeDescriptor.of[IntRecord]
    val tree = FunctionDef(
      List(ValueDef("a", input1), ValueDef("b", input2)),
      ApplyFunction(
        FunctionReference("FakeType", "fakeFunction"),
        List(SelectField(SelectTerm("a"), "i"), SelectField(SelectTerm("b"), "i")),
        types.String))

    val code = ScalarFunctionGenerator.default.getScalaAnonymousFunction(tree)
    val expectedCode = "(a: com.amazon.milan.compiler.scala.TestScalarFunctionGenerator.IntRecord, b: com.amazon.milan.compiler.scala.TestScalarFunctionGenerator.IntRecord) => FakeType.fakeFunction(a.i, b.i)"
    assertEquals(expectedCode, code)
  }

  @Test
  def test_ScalarFunctionGenerator_WithCreateInstance_ReturnsExpectedCode(): Unit = {
    val tree = FunctionDef(
      List(ValueDef("r", TypeDescriptor.of[IntRecord])),
      CreateInstance(createTypeDescriptor[IntRecord], List(SelectField(SelectTerm("r"), "i"))))

    val code = ScalarFunctionGenerator.default.getScalaAnonymousFunction(tree)
    val expectedCode = "(r: com.amazon.milan.compiler.scala.TestScalarFunctionGenerator.IntRecord) => new com.amazon.milan.compiler.scala.TestScalarFunctionGenerator.IntRecord(r.i)"
    assertEquals(expectedCode, code)
  }

  @Test
  def test_ScalarFunctionGenerator_WithUnpackOfTuple_ReturnsCodeUsingMatchStatement(): Unit = {
    val inputType = TypeDescriptor.createTuple[(Int, String)](List(types.Int, types.String))
    val tree = FunctionDef(List(ValueDef("t", inputType)), Unpack(SelectTerm("t"), List("i", "s"), SelectTerm("i")))
    TypeChecker.typeCheck(tree)

    val code = ScalarFunctionGenerator.default.getScalaAnonymousFunction(tree)
    val expectedCode = "(t: Tuple2[Int, String]) => t match { case Tuple2(i, s) => i }"
    assertEquals(expectedCode, code)
  }
}
