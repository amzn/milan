package com.amazon.milan.serialization

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import org.junit.Assert._
import org.junit.Test


object TestMilanObjectMapper {

  case class Record(i: Int)

  case class Record2(i: Int, s: String)

}

import com.amazon.milan.serialization.TestMilanObjectMapper._


@Test
class TestMilanObjectMapper {
  @Test
  def test_MilanObjectMapper_WithDefaultConfig_WithExtraPropertyInJson_IgnoresExtraProperty(): Unit = {
    val mapper = new MilanObjectMapper(DataFormatConfiguration.default)
    val json = mapper.writeValueAsString(Record2(2, "foo"))
    val o = mapper.readValue[Record](json, classOf[Record])
    assertEquals(2, o.i)
  }

  @Test(expected = classOf[UnrecognizedPropertyException])
  def test_MilanObjectMapper_WithFailOnUnknownProperties_WithExtraPropertyInJson_ThrowsUnrecognizedPropertyException(): Unit = {
    val mapper = new MilanObjectMapper(DataFormatConfiguration.withFlags(DataFormatFlags.FailOnUnknownProperties))
    val json = mapper.writeValueAsString(Record2(2, "foo"))
    val o = mapper.readValue[Record](json, classOf[Record])
  }

  @Test
  def test_MilanObjectMapper_WithOptionType_WithSomeValue_CopiesObject(): Unit = {
    val original = Some("test")
    val json = MilanObjectMapper.writerFor(classOf[Option[String]]).writeValueAsString(original)
    val reader = MilanObjectMapper.readerFor(classOf[Option[String]])
    val copy = reader.readValue[Option[String]](json)
    assertEquals(original, copy)
  }

  @Test
  def test_MilanObjectMapper_WithTupleType_CopiesObject(): Unit = {
    val original = (5, "test")
    val json = MilanObjectMapper.writerFor(classOf[(Int, String)]).writeValueAsString(original)
    val copy = MilanObjectMapper.readValue[(Int, String)](json)
    assertEquals(original, copy)
  }
}
