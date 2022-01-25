package com.amazon.milan.compiler.scala

import org.junit.Assert._
import org.junit.Test

@Test
class TestPackageObject {
  @Test
  def test_ToValidIdentifier_PrependsUnderscoreWhenStartsWithNumber(): Unit = {
    assertEquals("_1", toValidIdentifier("1"))
    assertEquals("_1abc", toValidIdentifier("1abc"))
  }

  @Test
  def test_ToValidName_ReplacesInvalidCharactersWithUnderscore(): Unit = {
    assertEquals("___________", toValidName("!@£$%^&*()-"))
  }

  @Test
  def test_ToValidName_LeavesValidIdentifierTheSame(): Unit = {
    assertEquals("validIdentifier", toValidName("validIdentifier"))
  }
}
