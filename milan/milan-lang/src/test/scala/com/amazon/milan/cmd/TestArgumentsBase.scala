package com.amazon.milan.cmd

import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable


object TestArgumentsBase {

  class ArgsOneRequiredStringArgument extends ArgumentsBase {
    @NamedArgument(Name = "string")
    var stringArgument: String = ""
  }


  class ArgsOneOptionalStringArgument extends ArgumentsBase {
    @NamedArgument(Name = "string", Required = false, DefaultValue = "default")
    var stringArgument: String = ""
  }


  class ArgsParameters extends ArgumentsBase {
    @NamedArgument(Name = "foo", Required = true)
    var foo: String = ""

    @ParametersArgument(Prefix = "P")
    val paramsP: mutable.ListBuffer[(String, String)] = mutable.ListBuffer.empty

    @ParametersArgument(Prefix = "C")
    val paramsC: mutable.ListBuffer[(String, String)] = mutable.ListBuffer.empty
  }

}

import com.amazon.milan.cmd.TestArgumentsBase._

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

  @Test
  def test_ArgumentsBase_Parse_WithParametersArgument_SetsParameters(): Unit = {
    val args = new ArgsParameters()
    args.parse(Array("--foo", "foo", "-Pa=b", "-Px=y", "-Cq=t"))
    assertEquals(args.foo, "foo")
    assertEquals(args.paramsP.find(_._1 == "a").get._2, "b")
    assertEquals(args.paramsP.find(_._1 == "x").get._2, "y")
    assertEquals(args.paramsC.find(_._1 == "q").get._2, "t")
  }
}
