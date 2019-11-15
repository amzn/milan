package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.program._
import com.amazon.milan.test.{IntRecord, KeyValueRecord, StringRecord}
import com.amazon.milan.typeutil.{TupleTypeDescriptor, TypeDescriptor, types}
import org.junit.Assert._
import org.junit.Test


@Test
class TestContextualTreeTransformer {
  @Test
  def test_ContextualTreeTransformer_Transform_ForFunctionWithOneArgumentThatReturnsThatArgument_ReturnsIdenticalFunction(): Unit = {
    val function = FunctionDef(List("x"), SelectTerm("x"))
    val transformed = ContextualTreeTransformer.transform(function, List(TypeDescriptor.of[IntRecord]))
    assertEquals(function, transformed)
  }

  @Test
  def test_ContextualTreeTransformer_Transform_ForFunctionWithOneTupleArgumentWithUnpackToTwoValues_ReturnsFunctionWithSelectFieldInsteadOfUnpack(): Unit = {
    val function = FunctionDef(List("x"), Unpack(SelectTerm("x"), List("a", "b"), SelectTerm("b")))
    val inputTypes = List(TypeDescriptor.createNamedTuple[(Int, Long)](List(("intField", TypeDescriptor.of[Int]), ("longField", TypeDescriptor.of[Long]))))
    val transformed = ContextualTreeTransformer.transform(function, inputTypes)
    val FunctionDef(List("x"), SelectField(SelectTerm("x"), "longField")) = transformed
  }

  @Test
  def test_ContextualTreeTransformer_Transform_ForFunctionWithOneObjectAndOneTupleArgumentThatUnpacksTheTupleArgument_ReturnsFunctionWithUnpackRemoved(): Unit = {
    val function = FunctionDef(List("a", "b"), Unpack(SelectTerm("b"), List("b1", "b2"), Equals(SelectField(SelectTerm("a"), "i"), SelectField(SelectTerm("b1"), "i"))))
    val inputTypes = List(
      TypeDescriptor.of[IntRecord],
      TypeDescriptor.createNamedTuple[(IntRecord, StringRecord)](List(("first", TypeDescriptor.of[IntRecord]), ("second", TypeDescriptor.of[StringRecord]))))

    val transformed = ContextualTreeTransformer.transform(function, inputTypes)
    val FunctionDef(List("a", "b"), Equals(SelectField(SelectTerm("a"), "i"), SelectField(SelectField(SelectTerm("b"), "first"), "i"))) = transformed
  }

  @Test
  def test_ContextualTreeTransformer_Transform_WithTwoArgumentFunction_WithOneRecordAndOneTupleArgument_ThatUnpacksTupleArgumentAndUsesAllFields_ReturnsExpectedTree(): Unit = {
    val function = Tree.fromExpression((c: KeyValueRecord, ab: (KeyValueRecord, KeyValueRecord)) => ab match {
      case (a, b) => a != null && b != null && c != null
    }).asInstanceOf[FunctionDef]
    val inputTypes = List(
      TypeDescriptor.of[IntRecord],
      TypeDescriptor.createNamedTuple[(IntRecord, IntRecord)](List(("a", TypeDescriptor.of[IntRecord]), ("b", TypeDescriptor.of[IntRecord]))))

    val transformed = ContextualTreeTransformer.transform(function, inputTypes)
    val FunctionDef(List("c", "ab"), And(And(Not(IsNull(SelectField(SelectTerm("ab"), "a"))), Not(IsNull(SelectField(SelectTerm("ab"), "b")))), Not(IsNull(SelectTerm("c"))))) = transformed
  }

  @Test
  def test_ContextualTreeTransformer_Transform_WithTupleArgumentWithoutFieldNamesAndSelectTermFromUnpack_ReturnsIdenticalTree(): Unit = {
    val function = Tree.fromFunction((t: (Int, String)) => t match {
      case (i, s) => i
    })
    val inputTypes = List(TypeDescriptor.createTuple[(Int, String)](List(types.Int, types.String)))
    TypeChecker.typeCheck(function, inputTypes)

    val transformed = ContextualTreeTransformer.transform(function, inputTypes)
    val FunctionDef(List("t"), Unpack(SelectTerm("t"), List("i", "s"), SelectTerm("i"))) = transformed
  }
}
