package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.program._
import com.amazon.milan.test.IntRecord
import com.amazon.milan.types.RecordIdFieldName
import com.amazon.milan.typeutil.{TupleTypeDescriptor, TypeDescriptor, createTypeDescriptor, types}
import org.junit.Assert._
import org.junit.Test


@Test
class TestTreeScalaConverter {
  @Test
  def test_TreeScalaConverter_WithTwoObjectInputs_ReturnsExpectedCode(): Unit = {
    val input1 = TypeDescriptor.of[IntRecord]
    val input2 = TypeDescriptor.of[IntRecord]
    val tree = FunctionDef(
      List("a", "b"),
      ApplyFunction(
        FunctionReference("FakeType", "fakeFunction"),
        List(SelectField(SelectTerm("a"), "i"), SelectField(SelectTerm("b"), "i")),
        types.String))

    val code = TreeScalaConverter.getScalaAnonymousFunction(tree, List(input1, input2))
    val expectedCode = "(a: com.amazon.milan.test.IntRecord, b: com.amazon.milan.test.IntRecord) => FakeType.fakeFunction(a.i, b.i)"
    assertEquals(expectedCode, code)
  }

  @Test
  def test_TreeScalaConverter_WithTwoTupleInputs_ReturnsExpectedCode(): Unit = {
    val input1 = TypeDescriptor.createNamedTuple[(Int, String)](List(("i", types.Int), ("s", types.String)))
    val input2 = TypeDescriptor.createNamedTuple[(Int, String)](List(("i", types.Int), ("s", types.String)))
    val tree = FunctionDef(
      List("a", "b"),
      ApplyFunction(
        FunctionReference("FakeType", "fakeFunction"),
        List(SelectField(SelectTerm("a"), "i"), SelectField(SelectTerm("b"), "s")),
        types.String))

    val code = TreeScalaConverter.getScalaAnonymousFunction(tree, List(input1, input2))
    val expectedCode = "(a: com.amazon.milan.flink.types.ArrayRecord, b: com.amazon.milan.flink.types.ArrayRecord) => FakeType.fakeFunction(a(0).asInstanceOf[Int], b(1).asInstanceOf[String])"
    assertEquals(expectedCode, code)
  }

  @Test
  def test_TreeScalaConverter_WithOneObjectAndOneTupleInput_ReturnsExpectedCode(): Unit = {
    val input1 = TypeDescriptor.of[IntRecord]
    val input2 = TypeDescriptor.createNamedTuple[(Int, String)](List(("i", types.Int), ("s", types.String)))
    val tree = FunctionDef(
      List("a", "b"),
      ApplyFunction(
        FunctionReference("FakeType", "fakeFunction"),
        List(SelectField(SelectTerm("a"), "i"), SelectField(SelectTerm("b"), "s")),
        types.String))

    val code = TreeScalaConverter.getScalaAnonymousFunction(tree, List(input1, input2))
    val expectedCode = "(a: com.amazon.milan.test.IntRecord, b: com.amazon.milan.flink.types.ArrayRecord) => FakeType.fakeFunction(a.i, b(1).asInstanceOf[String])"
    assertEquals(expectedCode, code)
  }

  @Test
  def test_TreeScalaConverter_WithCreateInstance_ReturnsExpectedCode(): Unit = {
    val input1 = TypeDescriptor.of[IntRecord]

    val tree = FunctionDef(
      List("r"),
      CreateInstance(createTypeDescriptor[IntRecord], List(SelectField(SelectTerm("r"), RecordIdFieldName), SelectField(SelectTerm("r"), "i"))))

    val code = TreeScalaConverter.getScalaAnonymousFunction(tree, input1)
    val expectedCode = "(r: com.amazon.milan.test.IntRecord) => new com.amazon.milan.test.IntRecord(r.recordId, r.i)"
    assertEquals(expectedCode, code)
  }

  @Test
  def test_TreeScalaConverter_WithUnpackOfTuple_ReturnsCodeWithMatchStatementAndOneCase(): Unit = {
    val tree = FunctionDef(List("t"), Unpack(SelectTerm("t"), List("i", "s"), SelectTerm("i")))
    val inputTypes = List(TypeDescriptor.createTuple[(Int, String)](List(types.Int, types.String)))
    TypeChecker.typeCheck(tree, inputTypes)

    val code = TreeScalaConverter.getScalaAnonymousFunction(tree, inputTypes)
    val expectedCode = "(t: Tuple2[Int, String]) => t match { case (i, s) => i }"
    assertEquals(expectedCode, code)
  }
}
