package com.amazon.milan.serialization

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import org.junit.Assert._
import org.junit.Test


object TestScalaObjectMapper {

  case class Record(i: Int)

  case class Record2(i: Int, s: String)

}

import com.amazon.milan.serialization.TestScalaObjectMapper._


@Test
class TestScalaObjectMapper {
  @Test
  def test_ScalaObjectMapper_WithDefaultConfig_WithExtraPropertyInJson_IgnoresExtraProperty(): Unit = {
    val mapper = new ScalaObjectMapper(DataFormatConfiguration.default)
    val json = mapper.writeValueAsString(Record2(2, "foo"))
    val o = mapper.readValue[Record](json, classOf[Record])
    assertEquals(2, o.i)
  }

  @Test(expected = classOf[UnrecognizedPropertyException])
  def test_ScalaObjectMapper_WithFailOnUnknownProperties_WithExtraPropertyInJson_ThrowsUnrecognizedPropertyException(): Unit = {
    val mapper = new ScalaObjectMapper(DataFormatConfiguration.withFlags(DataFormatFlags.FailOnUnknownProperties))
    val json = mapper.writeValueAsString(Record2(2, "foo"))
    val o = mapper.readValue[Record](json, classOf[Record])
  }
}
