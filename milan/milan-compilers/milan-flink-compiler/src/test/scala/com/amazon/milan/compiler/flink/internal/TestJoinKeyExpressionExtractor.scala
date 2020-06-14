package com.amazon.milan.compiler.flink.internal

import com.amazon.milan.program.Tree
import org.junit.Assert._
import org.junit.Test


object TestJoinKeyExpressionExtractor {

  case class Record(a: Int, b: String, c: Long, d: String)

  def functionOfRecord(r: Record): Boolean = true
}

import com.amazon.milan.compiler.flink.internal.TestJoinKeyExpressionExtractor._


@Test
class TestJoinKeyExpressionExtractor {
  @Test
  def test_JoinKeyExpressionExtractor_ExtractJoinKeyExpression_WithSingleFieldEqualityCheck_ReturnsInputExpressionAsExtractedAndNoRemainder(): Unit = {
    val tree = Tree.fromExpression((l: Record, r: Record) => l.a == r.a)
    val result = JoinKeyExpressionExtractor.extractJoinKeyExpression(tree)
    assertEquals(Some(tree), result.extracted)
    assertTrue(result.remainder.isEmpty)
  }

  @Test
  def test_JoinKeyExpressionExtractor_ExtractJoinKeyExpression_WithTwoFieldEqualityChecks_ReturnsInputExpressionAsExtractedAndNoRemainder(): Unit = {
    val tree = Tree.fromExpression((l: Record, r: Record) => (l.a == r.a) && (l.b == r.b))
    val result = JoinKeyExpressionExtractor.extractJoinKeyExpression(tree)
    assertEquals(Some(tree), result.extracted)
    assertTrue(result.remainder.isEmpty)
  }

  @Test
  def test_JoinKeyExpressionExtractor_ExtractJoinKeyExpression_WithFourFieldEqualityChecks_ReturnsInputExpressionAsExtractedAndNoRemainder(): Unit = {
    val tree = Tree.fromExpression((l: Record, r: Record) => (l.a == r.a) && (l.b == r.b) && (l.c == r.c) && (l.d == r.d))
    val result = JoinKeyExpressionExtractor.extractJoinKeyExpression(tree)
    assertEquals(Some(tree), result.extracted)
    assertTrue(result.remainder.isEmpty)
  }

  @Test
  def test_JoinKeyExpressionExtractor_ExtractJoinKeyExpression_WithFieldEqualityCheckAndFunctionOfOneInput_ReturnsEqualityCheckAsExtractedAndFunctionCallAsRemainder(): Unit = {
    val tree = Tree.fromExpression((l: Record, r: Record) => (l.a == r.a) && functionOfRecord(l))
    val result = JoinKeyExpressionExtractor.extractJoinKeyExpression(tree)
    assertEquals(Some(Tree.fromExpression((l: Record, r: Record) => l.a == r.a)), result.extracted)
    assertEquals(Some(Tree.fromExpression((l: Record, r: Record) => functionOfRecord(l))), result.remainder)
  }
}
