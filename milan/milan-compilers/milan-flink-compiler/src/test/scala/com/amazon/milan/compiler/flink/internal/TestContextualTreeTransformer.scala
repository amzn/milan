package com.amazon.milan.compiler.flink.internal

import com.amazon.milan.compiler.flink.testing.{IntRecord, KeyValueRecord, StringRecord}
import com.amazon.milan.program._
import com.amazon.milan.typeutil.{TypeDescriptor, types}
import org.junit.Assert._
import org.junit.Test


@Test
class TestContextualTreeTransformer {
  @Test
  def test_ContextualTreeTransformer_Transform_ForFunctionWithOneArgumentThatReturnsThatArgument_ReturnsIdenticalFunction(): Unit = {
    val function = FunctionDef(List(ValueDef("x", TypeDescriptor.of[IntRecord])), SelectTerm("x"))
    val transformed = ContextualTreeTransformer.transform(function)
    assertEquals(function, transformed)
  }

  @Test
  def test_ContextualTreeTransformer_Transform_ForFunctionWithOneTupleArgumentWithUnpackToTwoValues_ReturnsFunctionWithSelectFieldInsteadOfUnpack(): Unit = {
    val inputType = TypeDescriptor.createNamedTuple[(Int, Long)](List(("intField", TypeDescriptor.of[Int]), ("longField", TypeDescriptor.of[Long])))
    val function = FunctionDef(List(ValueDef("x", inputType)), Unpack(SelectTerm("x"), List("a", "b"), SelectTerm("b")))
    val transformed = ContextualTreeTransformer.transform(function)
    val FunctionDef(List(ValueDef("x", _)), SelectField(SelectTerm("x"), "longField")) = transformed
  }

  @Test
  def test_ContextualTreeTransformer_Transform_ForFunctionWithOneObjectAndOneTupleArgumentThatUnpacksTheTupleArgument_ReturnsFunctionWithUnpackRemoved(): Unit = {
    val input1 = TypeDescriptor.of[IntRecord]
    val input2 = TypeDescriptor.createNamedTuple[(IntRecord, StringRecord)](List(("first", TypeDescriptor.of[IntRecord]), ("second", TypeDescriptor.of[StringRecord])))
    val function = FunctionDef(List(ValueDef("a", input1), ValueDef("b", input2)), Unpack(SelectTerm("b"), List("b1", "b2"), Equals(SelectField(SelectTerm("a"), "i"), SelectField(SelectTerm("b1"), "i"))))

    val transformed = ContextualTreeTransformer.transform(function)
    val FunctionDef(List(ValueDef("a", _), ValueDef("b", _)), Equals(SelectField(SelectTerm("a"), "i"), SelectField(SelectField(SelectTerm("b"), "first"), "i"))) = transformed
  }

  @Test
  def test_ContextualTreeTransformer_Transform_WithTwoArgumentFunction_WithOneRecordAndOneTupleArgument_ThatUnpacksTupleArgumentAndUsesAllFields_ReturnsExpectedTree(): Unit = {
    val inputTypes = List(
      TypeDescriptor.of[IntRecord],
      TypeDescriptor.createNamedTuple[(IntRecord, IntRecord)](List(("a", TypeDescriptor.of[IntRecord]), ("b", TypeDescriptor.of[IntRecord]))))
    val function = Tree.fromFunction((c: KeyValueRecord, ab: (KeyValueRecord, KeyValueRecord)) => ab match {
      case (a, b) => a != null && b != null && c != null
    })
      .withArgumentTypes(inputTypes)

    val transformed = ContextualTreeTransformer.transform(function)
    val FunctionDef(List(ValueDef("c", _), ValueDef("ab", _)), And(And(Not(IsNull(SelectField(SelectTerm("ab"), "a"))), Not(IsNull(SelectField(SelectTerm("ab"), "b")))), Not(IsNull(SelectTerm("c"))))) = transformed
  }

  @Test
  def test_ContextualTreeTransformer_Transform_WithTupleArgumentWithoutFieldNamesAndSelectTermFromUnpack_ReturnsTreeUsingFlinkTupleField(): Unit = {
    val inputTypes = List(TypeDescriptor.createTuple[(Int, String)](List(types.Int, types.String)))
    val function = Tree.fromFunction((t: (Int, String)) => t match {
      case (i, s) => i
    })
      .withArgumentTypes(inputTypes)

    TypeChecker.typeCheck(function)

    val transformed = ContextualTreeTransformer.transform(function)
    val FunctionDef(List(ValueDef("t", _)), TupleElement(SelectTerm("t"), 0)) = transformed
  }
}
