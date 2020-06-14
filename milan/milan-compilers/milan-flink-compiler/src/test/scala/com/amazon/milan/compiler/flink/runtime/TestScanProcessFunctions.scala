package com.amazon.milan.compiler.flink.runtime

import com.amazon.milan.compiler.flink.testing.{SingletonMemorySinkFunction, _}
import com.amazon.milan.compiler.flink.testutil._
import com.amazon.milan.compiler.flink.types.{NoneTypeInformation, RecordWrapper}
import org.apache.flink.api.scala._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._


@Test
class TestScanProcessFunctions {
  @Test
  def test_ScanKeyedProcessFunction_ThatPerformsRollingSum_OutputsExpectedRecords(): Unit = {
    val function = new ScanKeyedProcessFunction[IntKeyValueRecord, Tuple1[Int], Int, IntKeyValueRecord](
      0,
      createTypeInformation[Tuple1[Int]],
      createTypeInformation[Int],
      createTypeInformation[IntKeyValueRecord]) {

      override protected def process(state: Int, key: Tuple1[Int], value: IntKeyValueRecord): (Int, Option[IntKeyValueRecord]) = {
        val sum = state + value.value
        (sum, Some(IntKeyValueRecord(key._1, sum)))
      }
    }

    val data = generateIntKeyValueRecords(1000, 10, 100)

    val env = getTestExecutionEnvironment

    val input = env.fromCollection(data.asJavaCollection, createTypeInformation[IntKeyValueRecord]).wrap(createTypeInformation[IntKeyValueRecord])

    val keySelector = new RecordWrapperKeySelector[IntKeyValueRecord, Tuple1[Int]](createTypeInformation[Tuple1[Int]])
    val keyed =
      input
        .map(new ModifyRecordKeyMapFunction[IntKeyValueRecord, Product, Tuple1[Int]](createTypeInformation[IntKeyValueRecord], createTypeInformation[Tuple1[Int]]) {
          override protected def getNewKey(value: IntKeyValueRecord, key: Product): Tuple1[Int] = Tuple1(value.key)
        })
        .keyBy(keySelector, keySelector.getKeyType)

    val output = keyed.process(function).unwrap(createTypeInformation[IntKeyValueRecord])

    val sink = new SingletonMemorySinkFunction[IntKeyValueRecord]()
    output.addSink(sink)

    env.executeThenWaitFor(() => sink.getRecordCount == 1000, 5)

    val actualOutput = sink.getValues
    val actualFinalRecords = actualOutput.groupBy(_.key).map { case (_, g) => g.last }.toList.sortBy(_.key)
    val expectedFinalRecords = data.groupBy(_.key).map { case (k, g) => IntKeyValueRecord(k, g.map(_.value).sum) }.toList.sortBy(_.key)

    assertEquals(expectedFinalRecords, actualFinalRecords)
  }

  @Test
  def test_ScanProcessFunction_ThatPerformsRollingSum_OutputsExpectedFinalValue(): Unit = {
    val function = new ScanProcessFunction[IntKeyValueRecord, Product, Int, IntRecord](
      0,
      NoneTypeInformation.instance,
      createTypeInformation[Int],
      createTypeInformation[IntRecord]) {
      override protected def process(state: Int, value: IntKeyValueRecord): (Int, Option[IntRecord]) = {
        val sum = state + value.value
        (sum, Some(IntRecord(sum)))
      }
    }

    val recordCount = 10
    val data = generateIntKeyValueRecords(recordCount, 10, 100)

    val env = getTestExecutionEnvironment

    val input = env.fromCollection(data.asJavaCollection, createTypeInformation[IntKeyValueRecord]).wrap(createTypeInformation[IntKeyValueRecord])
    val output = input.process(function).setParallelism(1)

    val sink = new SingletonMemorySinkFunction[RecordWrapper[IntRecord, Product]]()
    output.addSink(sink)

    env.executeThenWaitFor(() => sink.getRecordCount == recordCount, 5)

    val actualOutput = sink.getValues
    val actualFinalRecord = actualOutput.maxBy(_.sequenceNumber).value
    val expectedFinalRecord = IntRecord(data.map(_.value).sum)

    assertEquals(expectedFinalRecord, actualFinalRecord)
  }
}
