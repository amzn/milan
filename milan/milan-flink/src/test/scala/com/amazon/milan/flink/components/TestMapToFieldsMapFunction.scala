package com.amazon.milan.flink.components

import com.amazon.milan.flink.testutil._
import com.amazon.milan.flink.types.ArrayRecord
import com.amazon.milan.program._
import com.amazon.milan.program.testing._
import com.amazon.milan.typeutil.{TypeDescriptor, types}
import org.junit.Assert._
import org.junit.Test


@Test
class TestMapToFieldsMapFunction {
  @Test
  def test_MapToFieldsMapFunction_CanSerializeAndDeserializeViaObjectStream_ThenProduceSameOutputAsOriginalMapFunctionObject(): Unit = {
    val outputTypeInfo = TupleStreamTypeInformation.createFromFieldTypes(List(("a", "Int"), ("b", "Int")))

    val mapDef = MapFields(
      createRefFor[IntRecord],
      List(
        FieldDefinition("a", new FunctionDef(List("r"), new SelectField(SelectTerm("r"), "i"))),
        FieldDefinition("b", new FunctionDef(List("r"), new SelectField(SelectTerm("r"), "i")))
      ))

    val inputType = TypeDescriptor.of[IntRecord]

    val mapFunction = new MapToFieldsMapFunction[IntRecord](
      mapDef,
      inputType,
      outputTypeInfo,
      DoNothingMetricFactory)
    mapFunction.setRuntimeContext(MockRuntimeContext.create())

    // First make sure that the map function works.
    val expectedOutput = Array[Any](2, 2)
    val mapOutput = mapFunction.map(IntRecord(2))
    assertTrue(expectedOutput.sameElements(mapOutput.values))

    val mapFunctionCopy = ObjectStreamUtil.serializeAndDeserialize(mapFunction)
    mapFunctionCopy.setRuntimeContext(MockRuntimeContext.create())

    // Now make sure the copy works.
    val copyMapOutput = mapFunctionCopy.map(IntRecord(2))
    assertTrue(expectedOutput.sameElements(copyMapOutput.values))
  }

  @Test
  def test_MapToFieldsMapFunction_Map_WithObjectInputStream_OutputsMappedRecords(): Unit = {
    val outputTypeInfo = TupleStreamTypeInformation.createFromFieldTypes(List(("a", "Int"), ("b", "Int")))

    val mapDef = MapFields(
      createRefFor[IntRecord],
      List(
        FieldDefinition("a", new FunctionDef(List("r"), new SelectField(SelectTerm("r"), "i"))),
        FieldDefinition("b", new FunctionDef(List("r"), new SelectField(SelectTerm("r"), "i")))
      ))

    val inputType = TypeDescriptor.of[IntRecord]

    val mapFunction = new MapToFieldsMapFunction[IntRecord](
      mapDef,
      inputType,
      outputTypeInfo,
      DoNothingMetricFactory)
    mapFunction.setRuntimeContext(MockRuntimeContext.create())

    val expectedOutput = Array[Any](1, 1)
    val actualOutput = mapFunction.map(IntRecord(1))
    assertTrue(expectedOutput.sameElements(actualOutput.values))
  }

  @Test
  def test_MapToFieldsMapFunction_Map_WithTupleInputStream_OutputsMappedRecords(): Unit = {
    val outputTypeInfo = TupleStreamTypeInformation.createFromFieldTypes(List(("a", "Int"), ("b", "Int")))

    val inputType = TypeDescriptor.createNamedTuple[(Int, String)](List(("i", types.Int), ("s", types.String)))

    val mapDef = MapFields(
      createRef(inputType),
      List(
        FieldDefinition("a", new FunctionDef(List("r"), new SelectField(SelectTerm("r"), "s"))),
        FieldDefinition("b", new FunctionDef(List("r"), new SelectField(SelectTerm("r"), "i")))
      ))


    val mapFunction = new MapToFieldsMapFunction[ArrayRecord](
      mapDef,
      inputType,
      outputTypeInfo,
      DoNothingMetricFactory)
    mapFunction.setRuntimeContext(MockRuntimeContext.create())

    val inputRecord = ArrayRecord.fromElements(1, "foo")
    val expectedOutput = ArrayRecord.fromElements("foo", 1)
    val actualOutput = mapFunction.map(inputRecord)
    assertTrue(expectedOutput.sameElements(actualOutput.values))
  }
}
