package com.amazon.milan.flink.components

import com.amazon.milan.flink.testutil.{DoNothingMetricFactory, MockRuntimeContext}
import com.amazon.milan.flink.types.ArrayRecord
import com.amazon.milan.program._
import com.amazon.milan.program.testing._
import com.amazon.milan.test.{IntRecord, IntStringRecord}
import com.amazon.milan.typeutil.{TypeDescriptor, createTypeDescriptor, types}
import org.apache.flink.api.scala.createTypeInformation
import org.junit.Assert._
import org.junit.Test


@Test
class TestMapToRecordMapFunction {
  @Test
  def test_MapToRecordMapFunction_Map_WithObjectInputStream_OutputsMappedRecords(): Unit = {
    val mapExpr = new ApplyFunction(
      FunctionReference(createTypeDescriptor[IntRecord].fullName, "apply"),
      List(new SelectField(SelectTerm("r"), "i")),
      createTypeDescriptor[IntRecord])

    val inputType = TypeDescriptor.of[IntRecord]

    val mapDef = MapRecord(createRef(inputType), new FunctionDef(List("r"), mapExpr))

    val outputTypeInfo = createTypeInformation[IntRecord]

    val mapFunction = new MapToRecordMapFunction[IntRecord, IntRecord](
      mapDef,
      inputType,
      outputTypeInfo,
      DoNothingMetricFactory)
    mapFunction.setRuntimeContext(MockRuntimeContext.create())

    val actualOutput = mapFunction.map(IntRecord(1))
    assertEquals(IntRecord(1), actualOutput)
  }

  @Test
  def test_MapToRecordMapFunction_Map_WithTupleInputStream_OutputsMappedRecords(): Unit = {
    val mapExpr = new ApplyFunction(
      FunctionReference(createTypeDescriptor[IntStringRecord].fullName, "apply"),
      List(new SelectField(SelectTerm("r"), "i"), new SelectField(SelectTerm("r"), "s")),
      createTypeDescriptor[IntStringRecord])

    val inputType = TypeDescriptor.createNamedTuple[(Int, String)](List(("i", types.Int), ("s", types.String)))

    val mapDef = MapRecord(createRef(inputType), new FunctionDef(List("r"), mapExpr))

    val outputTypeInfo = createTypeInformation[IntStringRecord]

    val mapFunction = new MapToRecordMapFunction[ArrayRecord, IntStringRecord](
      mapDef,
      inputType,
      outputTypeInfo,
      DoNothingMetricFactory)
    mapFunction.setRuntimeContext(MockRuntimeContext.create())

    val actualOutput = mapFunction.map(ArrayRecord.fromElements(1, "foo"))
    assertEquals(IntStringRecord(1, "foo"), actualOutput)
  }
}
