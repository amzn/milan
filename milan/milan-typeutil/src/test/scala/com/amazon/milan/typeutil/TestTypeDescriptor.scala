package com.amazon.milan.typeutil

import com.amazon.milan.serialization.MilanObjectMapper
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test


object TestTypeDescriptor {

  case class Record()

  case class GenericRecord[T]()

}

import com.amazon.milan.typeutil.TestTypeDescriptor._


class TestTypeDescriptor {
  @Test
  def test_TypeDescriptor_Of_WithNonGenericRecordType_CreatesExpectedRecordTypeDescriptor(): Unit = {
    val target = TypeDescriptor.of[Record]
    assertTrue(target.genericArguments.isEmpty)
    assertEquals("com.amazon.milan.typeutil.TestTypeDescriptor.Record", target.typeName)
  }

  @Test
  def test_TypeDescriptor_Of_WithGenericRecordType_CreatesExpectedRecordTypeDescriptor(): Unit = {
    val target = TypeDescriptor.of[GenericRecord[Int]]
    assertEquals("com.amazon.milan.typeutil.TestTypeDescriptor.GenericRecord", target.typeName)
    assertEquals(1, target.genericArguments.length)
    assertEquals("Int", target.genericArguments.head.typeName)
  }

  @Test
  def test_TypeDescriptor_Of_WithGenericRecordTypeWithGenericArg_CreatesExpectedRecordTypeDescriptor(): Unit = {
    val target = TypeDescriptor.of[GenericRecord[List[Int]]]
    assertEquals("com.amazon.milan.typeutil.TestTypeDescriptor.GenericRecord", target.typeName)

    assertEquals(1, target.genericArguments.length)

    val genericArg = target.genericArguments.head
    assertEquals("List", genericArg.typeName)

    assertEquals(1, genericArg.genericArguments.length)
    assertEquals("Int", genericArg.genericArguments.head.typeName)
  }

  @Test
  def test_TypeDescriptor_Create_WithTupleType_ReturnsTypeDescriptorWithExpectedGenericArguments(): Unit = {
    val target = TypeDescriptor.forTypeName[Any]("Tuple3[Int, String, Long]")
    assertEquals("Tuple3", target.typeName)
    assertEquals(List(TypeDescriptor.of[Int], TypeDescriptor.of[String], TypeDescriptor.of[Long]), target.genericArguments)
  }

  @Test
  def test_TypeDescriptor_ForTypeName_ThenIsNumeric_WithNumericTypeNames_ReturnsTrue(): Unit = {
    val numericTypeNames = Seq("Double", "Float", "Int", "Long")
    numericTypeNames.foreach(name => assertTrue(TypeDescriptor.forTypeName[Any](name).isNumeric))
  }

  @Test
  def test_TupleTypeDescriptor_JsonSerializerAndDeserialize_ReturnsEquivalentType(): Unit = {
    val original = TypeDescriptor.of[(Int, String)]
    assertTrue(original.isTuple)

    val copy = MilanObjectMapper.copy(original)
    assertTrue(copy.isTuple)
    assertEquals(original, copy)
  }

  @Test
  def test_TypeDescriptor_NamedTupleOf_ReturnsExpectedFields(): Unit = {
    val actual = TypeDescriptor.namedTupleOf[(Int, String, Long)]("a", "b", "c")
    val expectedGenericArgs = List(types.Int, types.String, types.Long)
    val expectedFields = List(
      FieldDescriptor("a", types.Int),
      FieldDescriptor("b", types.String),
      FieldDescriptor("c", types.Long)
    )
    val expected = new TupleTypeDescriptor[(Int, String, Long)]("Tuple3", expectedGenericArgs, expectedFields)
    assertEquals(expected, actual)
  }

  @Test
  def test_TypeDescriptor_OfTuple_DoesntHaveFields(): Unit = {
    val target = TypeDescriptor.of[(Int, String)]
    assertTrue(target.fields.isEmpty)
  }
}
