package com.amazon.milan

import com.amazon.milan.serialization.MilanObjectMapper
import org.junit.Assert._
import org.junit.Test


@Test
class SemanticVersionTests {
  @Test
  def test_SemanticVersion_Comparison_WhenFirstLaterThanSecond(): Unit = {
    val examples = List(
      (SemanticVersion(1, 0, 0), SemanticVersion(0, 9, 0)),
      (SemanticVersion(1, 0, 0), SemanticVersion(0, 0, 1)),
      (SemanticVersion(2, 0, 0), SemanticVersion(1, 9, 9)),
      (SemanticVersion(1, 0, 0), SemanticVersion(1, 0, 0, "alpha")),
      (SemanticVersion(1, 0, 0, "beta"), SemanticVersion(1, 0, 0, "alpha")),
      (SemanticVersion(1, 0, 0, "alpha.1"), SemanticVersion(1, 0, 0, "alpha")),
      (SemanticVersion(1, 0, 0, "alpha.2"), SemanticVersion(1, 0, 0, "alpha.1")),
      (SemanticVersion(1, 0, 0, "alpha.1.x"), SemanticVersion(1, 0, 0, "alpha.1")),
      (SemanticVersion(1, 0, 0, "alpha.1.b"), SemanticVersion(1, 0, 0, "alpha.1.a"))
    )

    examples.foreach {
      case (first, second) =>
        val message = s"'$first' : '$second'"
        assertEquals(message, 1, first.compareTo(second))
        assertEquals(message, -1, second.compareTo(first))
        assertTrue(message, first > second)
        assertFalse(message, first < second)
        assertFalse(message, second > first)
        assertTrue(message, second < first)
        assertFalse(message, first == second)
    }
  }

  @Test
  def test_SemanticVersion_Comparison_WhenVersionNumbersEqual(): Unit = {
    val examples = List(
      (SemanticVersion(1, 0, 0), SemanticVersion(1, 0, 0)),
      (SemanticVersion(1, 0, 0, "alpha"), SemanticVersion(1, 0, 0, "alpha")),
      (SemanticVersion(1, 0, 0, "", "foo"), SemanticVersion(1, 0, 0, "", "bar")),
      (SemanticVersion(1, 0, 0, "alpha", "foo"), SemanticVersion(1, 0, 0, "alpha", "bar")),
      (SemanticVersion(1, 0, 0, "alpha", "foo"), SemanticVersion(1, 0, 0, "alpha", "foo"))
    )

    examples.foreach {
      case (first, second) =>
        val message = s"'$first' : '$second'"
        assertEquals(message, 0, first.compareTo(second))
        assertEquals(message, 0, second.compareTo(first))
        assertFalse(message, first > second)
        assertFalse(message, first < second)
        assertFalse(message, second > first)
        assertFalse(message, second < first)
    }
  }

  @Test
  def test_SemanticVersion_Comparison_WhenVersionNumbersEqualButObjectsNotEqual(): Unit = {
    val examples = List(
      (SemanticVersion(1, 0, 0, "alpha", "foo"), SemanticVersion(1, 0, 0, "alpha", "bar")),
      (SemanticVersion(1, 0, 0, "", "foo"), SemanticVersion(1, 0, 0, "", "bar"))
    )

    examples.foreach {
      case (first, second) =>
        val message = s"'$first' : '$second'"
        assertEquals(message, 0, first.compareTo(second))
        assertEquals(message, 0, second.compareTo(first))
        assertFalse(message, first > second)
        assertFalse(message, first < second)
        assertFalse(message, second > first)
        assertFalse(message, second < first)
        assertFalse(first.equals(second))
        assertFalse(first == second)
    }
  }

  @Test
  def test_SemanticVersion_JsonSerialization(): Unit = {
    val mapper = new MilanObjectMapper()

    val examples = List(
      SemanticVersion(1, 2, 3),
      SemanticVersion(1, 2, 3, "a"),
      SemanticVersion(1, 2, 3, null, "b"),
      SemanticVersion(1, 2, 3, "a", "b")
    )

    examples.foreach(
      expected => {
        val json = mapper.writeValueAsString(expected)
        val actual = mapper.readValue(json, classOf[SemanticVersion])
        assertEquals(expected, actual)
      }
    )
  }

  @Test
  def test_SemanticVersion_Parse(): Unit = {
    val examples = List(
      ("1.2.3", SemanticVersion(1, 2, 3)),
      ("1.2.3-alpha", SemanticVersion(1, 2, 3, "alpha")),
      ("1.2.3-a.b", SemanticVersion(1, 2, 3, "a.b")),
      ("1.2.3-a.b-c.d", SemanticVersion(1, 2, 3, "a.b-c.d")),
      ("1.2.3+a", SemanticVersion(1, 2, 3, null, "a")),
      ("1.2.3+a.b", SemanticVersion(1, 2, 3, null, "a.b")),
      ("1.2.3+a.b-c.d", SemanticVersion(1, 2, 3, null, "a.b-c.d")),
      ("1.2.3-a+b", SemanticVersion(1, 2, 3, "a", "b")),
      ("1.2.3-a.b+c", SemanticVersion(1, 2, 3, "a.b", "c")),
      ("1.2.3-a.b+c.d", SemanticVersion(1, 2, 3, "a.b", "c.d")),
      ("1.2.3-a.b-c+d", SemanticVersion(1, 2, 3, "a.b-c", "d")),
      ("1.2.3-a.b-c.d+e-f", SemanticVersion(1, 2, 3, "a.b-c.d", "e-f"))
    )

    examples.foreach {
      case (versionString, expected) =>
        val actual = SemanticVersion.parse(versionString)
        assertEquals(versionString, expected, actual)
    }
  }
}
