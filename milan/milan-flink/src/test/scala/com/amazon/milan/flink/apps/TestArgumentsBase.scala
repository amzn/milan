package com.amazon.milan.flink.apps

import org.junit.Assert._
import org.junit.Test


class ArgsOneRequiredStringArgument extends ArgumentsBase {
  @NamedArgument(Name = "string")
  var stringArgument: String = ""
}


class ArgsOneOptionalStringArgument extends ArgumentsBase {
  @NamedArgument(Name = "string", Required = false, DefaultValue = "default")
  var stringArgument: String = ""
}


@Test
class TestArgumentsBase {
  @Test
  def test_ArgumentsBase_Parse_WithLongNameOfStringArgument_ParsesTheArgument(): Unit = {
    val args = new ArgsOneRequiredStringArgument()
    args.parse(Array("--string", "value"))
    assertEquals("value", args.stringArgument)
  }

  @Test(expected = classOf[Exception])
  def test_ArgumentsBase_Parse_WithOneRequiredArgumentNotSupplied_ThrowsException(): Unit = {
    val args = new ArgsOneRequiredStringArgument()
    args.parse(Array())
  }

  @Test
  def test_ArgumentsBase_Parse_WithOneOptionalArgumentNotSupplied_HasDefaultArgumentValue(): Unit = {
    val args = new ArgsOneOptionalStringArgument()
    args.parse(Array())
    assertEquals("default", args.stringArgument)
  }
}
