package com.amazon.milan.serialization

import com.amazon.milan.test.IntRecord
import com.amazon.milan.typeutil.TypeDescriptor
import com.fasterxml.jackson.core.`type`.TypeReference
import org.junit.Assert._
import org.junit.Test


@Test
class TestJavaTypeFactory {
  @Test
  def test_JavaTypeFactory_MakeTypeReference_OfOptionType_ThenWriterFor_ProducesSameJsonAsWithoutWriter(): Unit = {
    val optionType = TypeDescriptor.of[Option[String]]
    val typeRef = new JavaTypeFactory(MilanObjectMapper.getTypeFactory).makeTypeReference(optionType)
    val item = Some("test")
    val json = MilanObjectMapper.writeValueAsString(item)
    val javaTypeJson = MilanObjectMapper.writerFor(typeRef).writeValueAsString(item)
    assertEquals(json, javaTypeJson)
  }

  @Test
  def test_JavaTypeFactory_MakeTypeReference_OfOptionType_ThenReaderFor_ReadsOptionValue(): Unit ={
    val original = Some(IntRecord(5))
    val json = MilanObjectMapper.writeValueAsString(original)

    val desc = TypeDescriptor.of[Option[IntRecord]]
    val descTypeRef = JavaTypeFactory.createDefault.makeTypeReference(desc).asInstanceOf[TypeReference[Option[IntRecord]]]
    val reader = MilanObjectMapper.readerFor(descTypeRef)
    val copy = reader.readValue[Option[IntRecord]](json)

    assertEquals(original, copy)
  }
}
