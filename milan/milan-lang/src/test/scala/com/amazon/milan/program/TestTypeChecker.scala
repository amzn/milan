package com.amazon.milan.program

import com.amazon.milan.lang.{field, fields}
import com.amazon.milan.test.IntRecord
import com.amazon.milan.typeutil.{TypeDescriptor, types}
import org.junit.Assert._
import org.junit.Test


@Test
class TestTypeChecker {
  @Test
  def test_TypeChecker_TypeCheck_MapRecordOfFlatMapOfGroupBy_DoesntThrowException(): Unit = {
    val expr = new StreamMap(
      new FlatMap(
        new GroupBy(
          new ExternalStream("input", "input", TypeDescriptor.streamOf[IntRecord]),
          FunctionDef.create(List("r"), SelectField(SelectTerm("r"), "i"))),
        FunctionDef.create(
          List("k", "g"),
          StreamArgMax(SelectTerm("g"), FunctionDef.create(List("r"), SelectField(SelectTerm("r"), "i"))))
      ),
      FunctionDef.create(List("r"), CreateInstance(TypeDescriptor.of[IntRecord], List(ConstantValue(1, types.Int))))
    )

    TypeChecker.typeCheck(expr, Map("input" -> TypeDescriptor.streamOf[IntRecord]))
  }

  @Test
  def test_TypeChecker_TypeCheck_WithFunctionUsingNamedFields_ProducesNamedTupleReturnType(): Unit = {
    val tree = Tree.fromFunction((i: Int) => fields(
      field("a", i + 1),
      field("b", i.toString)
    )).withArgumentTypes(List(types.Int))

    TypeChecker.typeCheck(tree)

    val expectedType = TypeDescriptor.createNamedTuple(List(("a", types.Int), ("b", types.String)))
    assertEquals(expectedType, tree.tpe)
  }
}
