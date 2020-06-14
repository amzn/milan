package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.application.ApplicationConfiguration
import com.amazon.milan.compiler.flink.testing._
import com.amazon.milan.lang.{Stream, StreamGraph, _}
import com.amazon.milan.testing.applications._
import org.junit.Assert.assertEquals
import org.junit.Test


object TestFlinkGenMap {
  def mapIntToString(i: IntRecord): StringRecord = StringRecord(i.i.toString)
}

import com.amazon.milan.compiler.flink.generator.TestFlinkGenMap._


class TestFlinkGenMap {
  @Test
  def test_FlinkGenMap_WithOneMappedRecordStream_OutputsMappedRecords(): Unit = {
    val input = Stream.of[IntRecord]
    val mapped = input.map(r => mapIntToString(r))

    val graph = new StreamGraph(mapped)

    val config = new ApplicationConfiguration
    config.setListSource(input, IntRecord(1), IntRecord(2), IntRecord(3))

    val result = TestApplicationExecutor.executeApplication(graph, config, 60, mapped)

    val expectedOutputRecords = List(StringRecord("1"), StringRecord("2"), StringRecord("3"))

    // The output records can arrive in a different order due to parallelism, so we need to sort them to be able to
    // test for equality between the lists.
    val outputRecords = result.getRecords(mapped).sortBy(_.s)

    assertEquals(expectedOutputRecords, outputRecords)
  }

  @Test
  def test_FlinkGenMap_WithOneMappedRecordStreamWithNamedField_OutputsMappedRecords(): Unit = {
    val input = Stream.of[IntRecord]
    val mapped = input.map(r => fields(
      field("i", r.i),
      field("s", r.i.toString)))

    val graph = new StreamGraph(mapped)

    val config = new ApplicationConfiguration
    config.setListSource(input, IntRecord(1), IntRecord(2), IntRecord(3))

    val result = TestApplicationExecutor.executeApplication(graph, config, 60, mapped)

    val expectedOutputRecords = List((1, "1"), (2, "2"), (3, "3"))

    // The output records can arrive in a different order due to parallelism, so we need to sort them to be able to
    // test for equality between the lists.
    val outputRecords = result.getRecords(mapped).sortBy(_._1)

    assertEquals(expectedOutputRecords, outputRecords)
  }

  @Test
  def test_FlinkGenMap_WithOneMappedTupleStreamWithOneField_OutputsMappedRecords(): Unit = {
    val input = Stream.of[IntRecord]
    val mapped = input.map(r => fields(field("i", r.i)))

    val graph = new StreamGraph(mapped)

    val config = new ApplicationConfiguration
    config.setListSource(input, IntRecord(1), IntRecord(2), IntRecord(3))

    val result = TestApplicationExecutor.executeApplication(graph, config, 60, mapped)

    val expectedOutputRecords = List(Tuple1(1), Tuple1(2), Tuple1(3))

    // The output records can arrive in a different order due to parallelism, so we need to sort them to be able to
    // test for equality between the lists.
    val outputRecords = result.getRecords(mapped).sortBy(_._1)

    assertEquals(expectedOutputRecords, outputRecords)
  }

}
