package com.amazon.milan.compiler.scala.event

import com.amazon.milan.compiler.scala.{DefaultTypeEmitter, RuntimeEvaluator, TypeLifter, ValName}
import com.amazon.milan.typeutil.{TupleTypeDescriptor, TypeDescriptor, types}
import org.junit.Assert._
import org.junit.Test


@Test
class TestKeyOperationsGenerator {
  private val generator = new KeyOperationsGenerator {
    override val typeLifter: TypeLifter = new TypeLifter(new DefaultTypeEmitter)
  }

  @Test
  def test_KeyOperationsGenerator_GenerateGetLocalKeyCode_WithBothKeysEmpty_ReturnsEmptyTuple(): Unit = {
    val getKeyCode = this.generator.generateGetLocalKeyCode(ValName("fullKey"), types.EmptyTuple, types.EmptyTuple)

    val code =
      s"""
         |val fullKey = Option.empty
         |$getKeyCode
         |""".stripMargin

    val localKey = RuntimeEvaluator.default.eval[Product](code)
    assertEquals(Option.empty, localKey)
  }

  @Test
  def test_KeyOperationsGenerator_GenerateGetLocalKeyCode_WithLocalKeyEmpty_ReturnsEmptyTuple(): Unit = {
    val fullKeyType = TypeDescriptor.of[(Int, String)]
    val keyType = types.EmptyTuple
    val getKeyCode = this.generator.generateGetLocalKeyCode(ValName("fullKey"), fullKeyType, keyType)

    val code =
      s"""
         |val fullKey = (1, "2")
         |$getKeyCode
         |""".stripMargin

    val localKey = RuntimeEvaluator.default.eval[Product](code)
    assertEquals(Option.empty, localKey)
  }

  @Test
  def test_KeyOperationsGenerator_GenerateGetLocalKeyCode_WithLocalKeyScalar_ReturnsLastElement(): Unit = {
    val fullKeyType = TypeDescriptor.of[(Int, String)]
    val keyType = types.String
    val getKeyCode = this.generator.generateGetLocalKeyCode(ValName("fullKey"), fullKeyType, keyType)

    val code =
      s"""
         |val fullKey = (1, "2")
         |$getKeyCode
         |""".stripMargin

    val localKey = RuntimeEvaluator.default.eval[String](code)
    assertEquals("2", localKey)
  }

  @Test
  def test_KeyOperationsGenerator_GenerateGetLocalKeyCode_WithLocalKeyTupleEqualToFullKey_ReturnsAllElements(): Unit = {
    val fullKeyType = TypeDescriptor.of[(Int, String)]
    val keyType = TypeDescriptor.of[(Int, String)]
    val getKeyCode = this.generator.generateGetLocalKeyCode(ValName("fullKey"), fullKeyType, keyType)

    val code =
      s"""
         |val fullKey = (1, "2")
         |$getKeyCode
         |""".stripMargin

    val localKey = RuntimeEvaluator.default.eval[(Int, String)](code)
    assertEquals((1, "2"), localKey)
  }

  @Test
  def test_KeyOperationsGenerator_GenerateGetLocalKeyCode_WithLocalKeyTuple_ReturnsLastElements(): Unit = {
    val fullKeyType = TypeDescriptor.of[(Int, Int, String)]
    val keyType = TypeDescriptor.of[(Int, String)]
    val getKeyCode = this.generator.generateGetLocalKeyCode(ValName("fullKey"), fullKeyType, keyType)

    val code =
      s"""
         |val fullKey = (1, 2, "3")
         |$getKeyCode
         |""".stripMargin

    val localKey = RuntimeEvaluator.default.eval[(Int, String)](code)
    assertEquals((2, "3"), localKey)
  }

  @Test
  def test_KeyOperationsGenerator_GenerateGetContextKeyCode_WithBothKeysEmpty_ReturnsEmptyTuple(): Unit = {
    val getKeyCode = this.generator.generateGetContextKeyCode(ValName("fullKey"), types.EmptyTuple, types.EmptyTuple)

    val code =
      s"""
         |val fullKey = Option.empty
         |$getKeyCode
         |""".stripMargin

    val contextKey = RuntimeEvaluator.default.eval[Product](code)
    assertEquals(Option.empty, contextKey)
  }

  @Test
  def test_KeyOperationsGenerator_GenerateGetContextKeyCode_WithContextKeyEmpty_ReturnsEmptyTuple(): Unit = {
    val fullKeyType = TypeDescriptor.of[(Int, String)]
    val contextKeyType = types.EmptyTuple
    val getKeyCode = this.generator.generateGetContextKeyCode(ValName("fullKey"), fullKeyType, contextKeyType)

    val code =
      s"""
         |val fullKey = (1, "2")
         |$getKeyCode
         |""".stripMargin

    val contextKey = RuntimeEvaluator.default.eval[Product](code)
    assertEquals(Option.empty, contextKey)
  }

  @Test
  def test_KeyOperationsGenerator_GenerateGetContextKeyCode_WithContextKeyScalar_ReturnsFirstElement(): Unit = {
    val fullKeyType = TypeDescriptor.of[(Int, String)]
    val contextKeyType = types.String
    val getKeyCode = this.generator.generateGetContextKeyCode(ValName("fullKey"), fullKeyType, contextKeyType)

    val code =
      s"""
         |val fullKey = (1, "2")
         |$getKeyCode
         |""".stripMargin

    val contextKey = RuntimeEvaluator.default.eval[Int](code)
    assertEquals(1, contextKey)
  }

  @Test
  def test_KeyOperationsGenerator_GenerateGetContextKeyCode_WithContextKeyTupleEqualToFullKey_ReturnsAllElements(): Unit = {
    val fullKeyType = TypeDescriptor.of[(Int, String)]
    val contextKeyType = TypeDescriptor.of[(Int, String)]
    val getKeyCode = this.generator.generateGetContextKeyCode(ValName("fullKey"), fullKeyType, contextKeyType)

    val code =
      s"""
         |val fullKey = (1, "2")
         |$getKeyCode
         |""".stripMargin

    val contextKey = RuntimeEvaluator.default.eval[(Int, String)](code)
    assertEquals((1, "2"), contextKey)
  }

  @Test
  def test_KeyOperationsGenerator_GenerateGetContextKeyCode_WithContextKeyTuple_ReturnsFirstElements(): Unit = {
    val fullKeyType = TypeDescriptor.of[(Int, Int, String)]
    val contextKeyType = TypeDescriptor.of[(Int, Int)]
    val getKeyCode = this.generator.generateGetContextKeyCode(ValName("fullKey"), fullKeyType, contextKeyType)

    val code =
      s"""
         |val fullKey = (1, 2, "3")
         |$getKeyCode
         |""".stripMargin

    val contextKey = RuntimeEvaluator.default.eval[(Int, Int)](code)
    assertEquals((1, 2), contextKey)
  }
}
