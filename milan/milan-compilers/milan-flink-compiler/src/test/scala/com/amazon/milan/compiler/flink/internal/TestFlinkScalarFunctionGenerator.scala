package com.amazon.milan.compiler.flink.internal

import com.amazon.milan.compiler.flink.testing.IntRecord
import com.amazon.milan.compiler.flink.typeutil._
import com.amazon.milan.program._
import com.amazon.milan.typeutil.{TypeDescriptor, types}
import org.junit.Assert._
import org.junit.Test


@Test
class TestFlinkScalarFunctionGenerator {
  @Test
  def test_FlinkScalarFunctionGenerator_WithTwoTupleRecordInputs_ReturnsExpectedCode(): Unit = {
    val input1 = TypeDescriptor.createNamedTuple[(Int, String)](List(("i", types.Int), ("s", types.String))).toTupleRecord
    val input2 = TypeDescriptor.createNamedTuple[(Int, String)](List(("i", types.Int), ("s", types.String))).toTupleRecord
    val tree = FunctionDef(
      List(ValueDef("a", input1), ValueDef("b", input2)),
      ApplyFunction(
        FunctionReference("FakeType", "fakeFunction"),
        List(SelectField(SelectTerm("a"), "i"), SelectField(SelectTerm("b"), "s")),
        types.String))

    val code = FlinkScalarFunctionGenerator.default.getScalaAnonymousFunction(tree)
    val expectedCode = "(a: com.amazon.milan.compiler.flink.types.ArrayRecord, b: com.amazon.milan.compiler.flink.types.ArrayRecord) => FakeType.fakeFunction(a(0).asInstanceOf[Int], b(1).asInstanceOf[String])"
    assertEquals(expectedCode, code)
  }

  @Test
  def test_FlinkScalarFunctionGenerator_WithOneObjectAndOneTupleRecordInput_ReturnsExpectedCode(): Unit = {
    val input1 = TypeDescriptor.of[IntRecord]
    val input2 = TypeDescriptor.createNamedTuple[(Int, String)](List(("i", types.Int), ("s", types.String))).toTupleRecord
    val tree = FunctionDef(
      List(ValueDef("a", input1), ValueDef("b", input2)),
      ApplyFunction(
        FunctionReference("FakeType", "fakeFunction"),
        List(SelectField(SelectTerm("a"), "i"), SelectField(SelectTerm("b"), "s")),
        types.String))

    val code = FlinkScalarFunctionGenerator.default.getScalaAnonymousFunction(tree)
    val expectedCode = "(a: com.amazon.milan.compiler.flink.testing.IntRecord, b: com.amazon.milan.compiler.flink.types.ArrayRecord) => FakeType.fakeFunction(a.i, b(1).asInstanceOf[String])"
    assertEquals(expectedCode, code)
  }

  @Test
  def test_FlinkScalarFunctionGenerator_WithUnpackOfTuple_ReturnsCodeUsingTupleField(): Unit = {
    val inputType = TypeDescriptor.createTuple[(Int, String)](List(types.Int, types.String))
    val tree = FunctionDef(List(ValueDef("t", inputType)), Unpack(SelectTerm("t"), List("i", "s"), SelectTerm("i")))
    TypeChecker.typeCheck(tree)

    val code = FlinkScalarFunctionGenerator.default.getScalaAnonymousFunction(tree)
    val expectedCode = "(t: Tuple2[Int, String]) => t._1"
    assertEquals(expectedCode, code)
  }
}
