package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.application.sinks.SingletonMemorySinkFunction
import com.amazon.milan.flink.testing._
import com.amazon.milan.flink.testutil._
import com.amazon.milan.flink.types.RecordWrapper
import org.apache.flink.api.scala._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.util.Random


@Test
class TestKeyedLastByOperator {
  @Test
  def test_KeyedLastByOperator_WithRandomInputsWithTenKeys_ReturnsOneRecordPerKeyWithMaxValue(): Unit = {
    val operator: KeyedLastByOperator[IntKeyValueRecord, Tuple1[Int]] = new KeyedLastByOperator[IntKeyValueRecord, Tuple1[Int]](createTypeInformation[IntKeyValueRecord], createTypeInformation[Tuple1[Int]]) {
      override protected def takeNewValue(newRecord: RecordWrapper[IntKeyValueRecord, Tuple1[Int]], currentRecord: RecordWrapper[IntKeyValueRecord, Tuple1[Int]]): Boolean = {
        newRecord.value.value > currentRecord.value.value
      }
    }

    val rand = new Random(0)
    val data = List.tabulate(1000)(_ => {
      IntKeyValueRecord(rand.nextInt(10), rand.nextInt(100))
    })

    val env = getTestExecutionEnvironment

    val input = env.fromCollection(data.asJavaCollection, createTypeInformation[IntKeyValueRecord]).wrap(createTypeInformation[IntKeyValueRecord])

    val keySelector = new RecordWrapperKeySelector[IntKeyValueRecord, Tuple1[Int]](createTypeInformation[Tuple1[Int]])
    val keyed =
      input
        .map(new ModifyRecordKeyMapFunction[IntKeyValueRecord, Product, Tuple1[Int]](createTypeInformation[IntKeyValueRecord], createTypeInformation[Tuple1[Int]]) {
          override protected def getNewKey(value: IntKeyValueRecord, key: Product): Tuple1[Int] = Tuple1(value.key)
        })
        .keyBy(keySelector, keySelector.getKeyType)

    val output = keyed.transform(
      "op",
      operator.getProducedType,
      operator)
      .unwrap()

    val sink = new SingletonMemorySinkFunction[IntKeyValueRecord]()
    output.addSink(sink)

    env.executeThenWaitFor(() => sink.getRecordCount >= 10, 5)

    val expectedOutput = data.groupBy(_.key).map { case (_, g) => g.maxBy(_.value) }.toList.sortBy(_.key)
    val actualOutput = sink.getValues.sortBy(_.key)

    assertEquals(expectedOutput, actualOutput)
  }
}
