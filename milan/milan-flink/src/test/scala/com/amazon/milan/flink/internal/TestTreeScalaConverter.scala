package com.amazon.milan.flink.internal

import com.amazon.milan.flink.testing.IntRecord
import com.amazon.milan.flink.typeutil._
import com.amazon.milan.program._
import com.amazon.milan.types.RecordIdFieldName
import com.amazon.milan.typeutil.{TypeDescriptor, createTypeDescriptor, types}
import org.junit.Assert._
import org.junit.Test


@Test
class TestTreeScalaConverter {
  @Test
  def test_TreeScalaConverter_WithTwoObjectInputs_ReturnsExpectedCode(): Unit = {
    val input1 = TypeDescriptor.of[IntRecord]
    val input2 = TypeDescriptor.of[IntRecord]
    val tree = FunctionDef(
      List(ValueDef("a", input1), ValueDef("b", input2)),
      ApplyFunction(
        FunctionReference("FakeType", "fakeFunction"),
        List(SelectField(SelectTerm("a"), "i"), SelectField(SelectTerm("b"), "i")),
        types.String))

    val code = TreeScalaConverter.getScalaAnonymousFunction(tree)
    val expectedCode = "(a: com.amazon.milan.flink.testing.IntRecord, b: com.amazon.milan.flink.testing.IntRecord) => FakeType.fakeFunction(a.i, b.i)"
    assertEquals(expectedCode, code)
  }

  @Test
  def test_TreeScalaConverter_WithTwoTupleRecordInputs_ReturnsExpectedCode(): Unit = {
    val input1 = TypeDescriptor.createNamedTuple[(Int, String)](List(("i", types.Int), ("s", types.String))).toTupleRecord
    val input2 = TypeDescriptor.createNamedTuple[(Int, String)](List(("i", types.Int), ("s", types.String))).toTupleRecord
    val tree = FunctionDef(
      List(ValueDef("a", input1), ValueDef("b", input2)),
      ApplyFunction(
        FunctionReference("FakeType", "fakeFunction"),
        List(SelectField(SelectTerm("a"), "i"), SelectField(SelectTerm("b"), "s")),
        types.String))

    val code = TreeScalaConverter.getScalaAnonymousFunction(tree)
    val expectedCode = "(a: com.amazon.milan.flink.types.ArrayRecord, b: com.amazon.milan.flink.types.ArrayRecord) => FakeType.fakeFunction(a(0).asInstanceOf[Int], b(1).asInstanceOf[String])"
    assertEquals(expectedCode, code)
  }

  @Test
  def test_TreeScalaConverter_WithOneObjectAndOneTupleRecordInput_ReturnsExpectedCode(): Unit = {
    val input1 = TypeDescriptor.of[IntRecord]
    val input2 = TypeDescriptor.createNamedTuple[(Int, String)](List(("i", types.Int), ("s", types.String))).toTupleRecord
    val tree = FunctionDef(
      List(ValueDef("a", input1), ValueDef("b", input2)),
      ApplyFunction(
        FunctionReference("FakeType", "fakeFunction"),
        List(SelectField(SelectTerm("a"), "i"), SelectField(SelectTerm("b"), "s")),
        types.String))

    val code = TreeScalaConverter.getScalaAnonymousFunction(tree)
    val expectedCode = "(a: com.amazon.milan.flink.testing.IntRecord, b: com.amazon.milan.flink.types.ArrayRecord) => FakeType.fakeFunction(a.i, b(1).asInstanceOf[String])"
    assertEquals(expectedCode, code)
  }

  @Test
  def test_TreeScalaConverter_WithCreateInstance_ReturnsExpectedCode(): Unit = {
    val tree = FunctionDef(
      List(ValueDef("r", TypeDescriptor.of[IntRecord])),
      CreateInstance(createTypeDescriptor[IntRecord], List(SelectField(SelectTerm("r"), RecordIdFieldName), SelectField(SelectTerm("r"), "i"))))

    val code = TreeScalaConverter.getScalaAnonymousFunction(tree)
    val expectedCode = "(r: com.amazon.milan.flink.testing.IntRecord) => new com.amazon.milan.flink.testing.IntRecord(r.recordId, r.i)"
    assertEquals(expectedCode, code)
  }

  @Test
  def test_TreeScalaConverter_WithUnpackOfTuple_ReturnsCodeUsingScalaTupleField(): Unit = {
    val inputType = TypeDescriptor.createTuple[(Int, String)](List(types.Int, types.String))
    val tree = FunctionDef(List(ValueDef("t", inputType)), Unpack(SelectTerm("t"), List("i", "s"), SelectTerm("i")))
    TypeChecker.typeCheck(tree)

    val code = TreeScalaConverter.forFlinkTypes.getScalaAnonymousFunction(tree)
    val expectedCode = "(t: Tuple2[Int, String]) => t._1"
    assertEquals(expectedCode, code)
  }
}
