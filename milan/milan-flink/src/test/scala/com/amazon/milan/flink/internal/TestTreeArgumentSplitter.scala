package com.amazon.milan.flink.internal

import com.amazon.milan.program.Tree
import org.junit.Assert._
import org.junit.Test


object TestTreeArgumentSplitter {

  case class Record(i: Int)

  def functionOfRecord(r: Record): Boolean = true

  def functionOfInt(i: Int): Boolean = true
}

import com.amazon.milan.flink.internal.TestTreeArgumentSplitter._


@Test
class TestTreeArgumentSplitter {
  @Test
  def test_TreeArgumentSplitter_SplitTree_WithTwoInputRecordsAndOneEqualsStatementForEachRecord_SplitsTheExpression(): Unit = {
    val tree = Tree.fromExpression((a: Record, b: Record) => a.i == 1 && b.i == 2)
    val result = TreeArgumentSplitter.splitTree(tree, "a")
    assertEquals(Some(Tree.fromExpression((a: Record, b: Record) => a.i == 1)), result.extracted)
    assertEquals(Some(Tree.fromExpression((a: Record, b: Record) => b.i == 2)), result.remainder)
  }

  @Test
  def test_TreeArgumentSplitter_SplitTree_WithTwoInputRecordsAndOneFunctionCallForEachRecord_SplitsTheExpression(): Unit = {
    val tree = Tree.fromExpression((a: Record, b: Record) => functionOfRecord(a) && functionOfInt(b.i))
    val result = TreeArgumentSplitter.splitTree(tree, "a")
    assertEquals(Some(Tree.fromExpression((a: Record, b: Record) => functionOfRecord(a))), result.extracted)
    assertEquals(Some(Tree.fromExpression((a: Record, b: Record) => functionOfInt(b.i))), result.remainder)
  }
}
