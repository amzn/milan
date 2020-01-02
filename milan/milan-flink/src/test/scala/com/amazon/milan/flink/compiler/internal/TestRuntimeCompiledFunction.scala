package com.amazon.milan.flink.compiler.internal


import com.amazon.milan.flink.testutil._
import com.amazon.milan.lang.aggregation._
import com.amazon.milan.program.{ArgMin, ConstantValue, FunctionDef, Mean, Plus, SelectField, SelectTerm, Sum, Tuple}
import com.amazon.milan.typeutil.{TypeDescriptor, types}
import org.junit.Assert._
import org.junit.Test


@Test
class TestRuntimeCompiledFunction {
  @Test
  def test_RuntimeCompilerFunction_HeadOfIterable(): Unit = {
    val f = RuntimeCompiledFunction.create((it: Iterable[Int]) => it.head)
    assertEquals(1, f(Seq(1, 2, 3)))
  }

  @Test
  def test_RuntimeCompiledFunction_SumOfValues(): Unit = {
    val f = RuntimeCompiledFunction.create((it: Iterable[Int]) => sum(it))
    assertEquals(6, f(Seq(1, 2, 3)))
  }

  @Test
  def test_RuntimeCompiledFunction_SumOfField(): Unit = {
    val function = FunctionDef(List("x"), Sum(SelectField(SelectTerm("x"), "i")))
    val inputType = TypeDescriptor.of[Iterable[IntRecord]]
    val target = new RuntimeCompiledFunction[Iterable[IntRecord], Int](inputType, function)
    val inputValues = List(IntRecord(1), IntRecord(2), IntRecord(3))
    assertEquals(6, target(inputValues))
  }

  @Test
  def test_RuntimeCompiledFunction_ArgMinOfFields(): Unit = {
    val function = FunctionDef(List("x"), ArgMin(Tuple(List(SelectField(SelectTerm("x"), "key"), SelectField(SelectTerm("x"), "value")))))
    val inputType = TypeDescriptor.of[Iterable[IntKeyValueRecord]]
    val target = new RuntimeCompiledFunction[Iterable[IntKeyValueRecord], Int](inputType, function)
    val inputValues = List(IntKeyValueRecord(2, 6), IntKeyValueRecord(1, 5), IntKeyValueRecord(3, 4))
    assertEquals(5, target(inputValues))
  }

  @Test
  def test_RuntimeCompiledFunction_MeanOfField(): Unit = {
    val function = FunctionDef(List("x"), Mean(SelectField(SelectTerm("x"), "i")))
    val inputType = TypeDescriptor.of[Iterable[IntRecord]]
    val target = new RuntimeCompiledFunction[Iterable[IntRecord], Double](inputType, function)
    val inputValues = List(IntRecord(1), IntRecord(2), IntRecord(4))
    assertEquals((1 + 2 + 4) / 3.0, target(inputValues), 1e-10)
  }

  @Test
  def test_RuntimeCompiledFunction_SumOfFieldPlusConstant(): Unit = {
    val function = FunctionDef(List("x"), Sum(Plus(SelectField(SelectTerm("x"), "i"), ConstantValue(3, types.Int))))
    val inputType = TypeDescriptor.of[Iterable[IntRecord]]
    val target = new RuntimeCompiledFunction[Iterable[IntRecord], Int](inputType, function)
    val inputValues = List(IntRecord(1), IntRecord(2), IntRecord(3))
    assertEquals(15, target(inputValues))
  }
}
