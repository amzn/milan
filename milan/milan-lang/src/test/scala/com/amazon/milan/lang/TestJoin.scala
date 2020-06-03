package com.amazon.milan.lang

import com.amazon.milan.program._
import com.amazon.milan.test.{IntKeyValueRecord, KeyValueRecord}
import com.amazon.milan.typeutil.{FieldDescriptor, TypeDescriptor, types}
import org.junit.Assert._
import org.junit.Test


object TestJoin {
  def getKey(r: KeyValueRecord): String = r.key

  def getValue(r: KeyValueRecord): String = r.value

  def joinRecords(left: KeyValueRecord, right: KeyValueRecord) = KeyValueRecord(left.key, left.value + ", " + right.value)

  def combineValues(key: String, leftValue: String, rightValue: String) = KeyValueRecord(key, leftValue + "." + rightValue)
}


@Test
class TestJoin {
  @Test
  def test_Join_WithTwoObjectStreams_ReturnsStreamWithExpectedNodeValues(): Unit = {
    val left = Stream.of[KeyValueRecord]
    val right = Stream.of[KeyValueRecord]

    val join = left.fullJoin(right)

    assertEquals(JoinType.FullEnrichmentJoin, join.joinType)
    assertEquals(left.expr, join.leftInput)
    assertEquals(right.expr, join.rightInput)
  }

  @Test
  def test_Join_ThenWhere_WithTwoObjectStreams_ReturnsStreamWithExpectedNodeValues(): Unit = {
    val left = Stream.of[KeyValueRecord]
    val right = Stream.of[KeyValueRecord]

    val join = left.fullJoin(right)
    val where = join.where((l, r) => l.key == r.key)

    val FullJoin(leftInput, rightInput, condition) = where.expr
    assertEquals(left.expr, leftInput)
    assertEquals(right.expr, rightInput)

    // Extract out the join condition expression.
    val FunctionDef(_, Equals(SelectField(SelectTerm("l"), "key"), SelectField(SelectTerm("r"), "key"))) = condition
  }

  @Test
  def test_Join_ThenWhere_ThenSelect_ToObjectStream_WithTwoObjectStreams_ReturnsStreamWithExpectedNodeValues(): Unit = {
    val left = Stream.of[KeyValueRecord]
    val right = Stream.of[KeyValueRecord]

    val join = left.fullJoin(right)
    val where = join.where((l, r) => l.key == r.key)
    val select = where.select((l, r) => TestJoin.joinRecords(l, r))

    val mapExpr = select.expr.asInstanceOf[StreamMap]
    assertEquals(where.expr, mapExpr.source)

    val FunctionDef(List(ValueDef("l", _), ValueDef("r", _)), ApplyFunction(FunctionReference(objectTypeName, "joinRecords"), List(SelectTerm("l"), SelectTerm("r")), _)) = mapExpr.expr
    assertEquals(classOf[TestJoin].getName, objectTypeName)
  }

  @Test
  def test_Join_ThenWhere_ThenSelect_ToTupleStream_WithTwoObjectStreams_ReturnsStreamWithExpectedNodeValues(): Unit = {
    val left = Stream.of[KeyValueRecord]
    val right = Stream.of[KeyValueRecord]

    val join = left.fullJoin(right)
    val where = join.where((l, r) => l.key == r.key)
    val select = where.select((l, r) => fields(field("j", TestJoin.joinRecords(l, r))))

    val mapExpr = select.expr.asInstanceOf[StreamMap]
    assertEquals(where.expr, mapExpr.source)


    val FunctionDef(List(ValueDef("l", _), ValueDef("r", _)), NamedFields(List(NamedField("j", ApplyFunction(FunctionReference(objectTypeName, "joinRecords"), List(SelectTerm("l"), SelectTerm("r")), _))))) = mapExpr.expr
    assertEquals(classOf[TestJoin].getName, objectTypeName)
  }

  @Test
  def test_Join_ThenWhere_ThenSelectAll_WithTwoObjectStreams_ReturnsStreamWithExpectedFieldsAndMapExpression(): Unit = {
    val left = Stream.of[IntKeyValueRecord]
    val right = Stream.of[IntKeyValueRecord]
    val joined = left.fullJoin(right).where((l, r) => l.key == r.key)
    val output = joined.selectAll()

    val expectedFields = List(
      FieldDescriptor("left", TypeDescriptor.of[IntKeyValueRecord]),
      FieldDescriptor("right", TypeDescriptor.of[IntKeyValueRecord]))
    assertEquals(expectedFields, output.recordType.fields)

    val StreamMap(_, FunctionDef(_, NamedFields(fields))) = output.expr
    val NamedField("left", SelectTerm("l")) = fields.head
    val NamedField("right", SelectTerm("r")) = fields.last
  }

  @Test
  def test_Join_ThenWhere_ThenSelectAll_WithObjectAndTupleStreams_ReturnsStreamWithExpectedFieldsAndMapExpression(): Unit = {
    val left = Stream.of[IntKeyValueRecord]
    val right = Stream.ofFields[(Int, Long)]("a", "b")
    val joined = left.fullJoin(right).where((l, r) => r match {
      case (a, b) => l.key == a
    })
    val output = joined.selectAll()

    val expectedFields = List(
      FieldDescriptor("left", TypeDescriptor.of[IntKeyValueRecord]),
      FieldDescriptor("a", types.Int),
      FieldDescriptor("b", types.Long))
    assertEquals(expectedFields, output.recordType.fields)

    val StreamMap(_, FunctionDef(_, NamedFields(fields))) = output.expr
    val NamedField("left", SelectTerm("l")) = fields.head
    val NamedField("a", SelectField(SelectTerm("r"), "a")) = fields(1)
    val NamedField("b", SelectField(SelectTerm("r"), "b")) = fields(2)
  }

  @Test
  def test_Join_ThenWhere_ThenSelectAll_WithTupleAndObjectStreams_ReturnsStreamWithExpectedFieldsAndMapExpression(): Unit = {
    val left = Stream.ofFields[(Int, Long)]("a", "b")
    val right = Stream.of[IntKeyValueRecord]
    val joined = left.fullJoin(right).where((l, r) => l match {
      case (a, b) => r.key == a
    })
    val output = joined.selectAll()

    val expectedFields = List(
      FieldDescriptor("a", types.Int),
      FieldDescriptor("b", types.Long),
      FieldDescriptor("right", TypeDescriptor.of[IntKeyValueRecord]))
    assertEquals(expectedFields, output.recordType.fields)

    val StreamMap(_, FunctionDef(_, NamedFields(fields))) = output.expr
    val NamedField("a", SelectField(SelectTerm("l"), "a")) = fields.head
    val NamedField("b", SelectField(SelectTerm("l"), "b")) = fields(1)
    val NamedField("right", SelectTerm("r")) = fields(2)
  }

  @Test
  def test_Join_ThenWhere_ThenSelectAll_WithTwoTupleStreams_ReturnsStreamWithExpectedFieldsAndMapExpression(): Unit = {
    val left = Stream.ofFields[(Int, Long)]("a", "b")
    val right = Stream.ofFields[(Int, Double)]("c", "d")
    val joined = left.fullJoin(right).where((l, r) => l match {
      case (a, b) => r match {
        case (c, d) => a == c
      }
    })
    val output = joined.selectAll()

    val expectedFields = List(
      FieldDescriptor("a", types.Int),
      FieldDescriptor("b", types.Long),
      FieldDescriptor("c", types.Int),
      FieldDescriptor("d", types.Double))
    assertEquals(expectedFields, output.recordType.fields)

    val StreamMap(_, FunctionDef(_, NamedFields(fields))) = output.expr
    val NamedField("a", SelectField(SelectTerm("l"), "a")) = fields.head
    val NamedField("b", SelectField(SelectTerm("l"), "b")) = fields(1)
    val NamedField("c", SelectField(SelectTerm("r"), "c")) = fields(2)
    val NamedField("d", SelectField(SelectTerm("r"), "d")) = fields(3)
  }
}
