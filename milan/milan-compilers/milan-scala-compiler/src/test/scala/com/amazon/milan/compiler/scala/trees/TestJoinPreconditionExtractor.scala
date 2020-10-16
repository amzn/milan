package com.amazon.milan.compiler.scala.trees

import com.amazon.milan.program.Tree
import org.junit.Assert._
import org.junit.Test


object TestJoinPreconditionExtractor {

  case class Record(i: Int)

}

import com.amazon.milan.compiler.scala.trees.TestJoinPreconditionExtractor._


@Test
class TestJoinPreconditionExtractor {
  @Test
  def test_JoinPredictionExtractor_ExtractJoinPrecondition_WithNotNullExpression_ReturnsInputExpressionAsRemainder(): Unit = {
    val tree = Tree.fromExpression((r: Record) => r != null)
    val result = JoinPreconditionExtractor.extractJoinPrecondition(tree)
    assertTrue(result.extracted.isEmpty)
    assertEquals(Some(tree), result.remainder)
  }

  @Test
  def test_JoinPredictionExtractor_ExtractJoinPrecondition_WithFieldEqualityExpression_ReturnsInputExpressionAsRemainder(): Unit = {
    val tree = Tree.fromExpression((a: Record, b: Record) => a.i == b.i)
    val result = JoinPreconditionExtractor.extractJoinPrecondition(tree)
    assertTrue(result.extracted.isEmpty)
    assertEquals(Some(tree), result.remainder)
  }

  @Test
  def test_JoinPredictionExtractor_ExtractJoinPrecondition_WithMixedExpression_ReturnsExpectedOutputs(): Unit = {
    val tree = Tree.fromExpression((a: Record, b: Record) => (a != null) && (a.i == b.i) && (a.i == 2) && (b.i == 3))
    val result = JoinPreconditionExtractor.extractJoinPrecondition(tree)
    assertEquals(Some(Tree.fromExpression((a: Record, b: Record) => (a.i == 2) && (b.i == 3))), result.extracted)
    assertEquals(Some(Tree.fromExpression((a: Record, b: Record) => (a != null) && (a.i == b.i))), result.remainder)
  }
}
