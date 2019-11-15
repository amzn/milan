package com.amazon.milan.typeutil

import org.junit.Assert._
import org.junit.Test


@Test
class TestTypeJoiner {
  @Test
  def test_TypeJoiner_WithTwoObjectTypes_ReturnsTupleWithTwoFields(): Unit = {
    val joiner = createTypeJoiner[Int, String]
    val joined = joiner.getOutputType(types.Int, types.String)
    val expected = TypeDescriptor.namedTupleOf[(Int, String)]("left", "right")
    assertEquals(expected, joined)
  }

  @Test
  def test_TypeJoiner_WithTwoTuples_ReturnsTupleWithCombinedFields(): Unit = {
    val joiner = createTypeJoiner[(Int, String), (Long, Double)]

    val leftType = TypeDescriptor.namedTupleOf[(Int, String)]("a", "b")
    val rightType = TypeDescriptor.namedTupleOf[(Long, Double)]("c", "d")
    val joined = joiner.getOutputType(leftType, rightType)

    val expected = TypeDescriptor.namedTupleOf[(Int, String, Long, Double)]("a", "b", "c", "d")
    assertEquals(expected, joined)
  }

  @Test
  def test_TypeJoiner_WithTwoTuples_WithOneNameCollision_AddsPrefixToRightFieldInOutput(): Unit = {
    val joiner = createTypeJoiner[(Int, String), (Long, Double)]

    val leftType = TypeDescriptor.namedTupleOf[(Int, String)]("a", "b")
    val rightType = TypeDescriptor.namedTupleOf[(Long, Double)]("b", "c")
    val joined = joiner.getOutputType(leftType, rightType)

    val expected = TypeDescriptor.namedTupleOf[(Int, String, Long, Double)]("a", "b", "right_b", "c")
    assertEquals(expected, joined)
  }

  @Test
  def test_TypeJoiner_WithLeftObjectAndRightTuple_ReturnsTupleWithCombinedFields(): Unit = {
    val joiner = createTypeJoiner[Int, (Long, Double)]

    val leftType = types.Int
    val rightType = TypeDescriptor.namedTupleOf[(Long, Double)]("c", "d")
    val joined = joiner.getOutputType(leftType, rightType)

    val expected = TypeDescriptor.namedTupleOf[(Int, Long, Double)]("left", "c", "d")
    assertEquals(expected, joined)
  }

  @Test
  def test_TypeJoiner_WithLeftTupleAndRightObject_ReturnsTupleWithCombinedFields(): Unit = {
    val joiner = createTypeJoiner[(Int, String), Double]

    val leftType = TypeDescriptor.namedTupleOf[(Int, String)]("a", "b")
    val rightType = types.Double
    val joined = joiner.getOutputType(leftType, rightType)

    val expected = TypeDescriptor.namedTupleOf[(Int, String, Double)]("a", "b", "right")
    assertEquals(expected, joined)
  }
}
