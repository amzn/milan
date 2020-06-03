package com.amazon.milan.flink.types

import com.amazon.milan.flink.runtime.SequenceNumberOrdering
import com.amazon.milan.flink.testing.IntRecord
import com.amazon.milan.flink.testutil._
import com.amazon.milan.flink.types
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable
import scala.util.Random


@Test
class TestPriorityQueueTypeSerializer {
  @Test
  def test_PriorityQueueTypeSerializer_Deserialize_WithQueueOfInt_With100RandomItems_ReturnsQueueThatYieldsSameItemsAsOriginal(): Unit = {
    val typeInfo = new PriorityQueueTypeInformation[Int](createTypeInformation[Int], Ordering.Int)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val serializer = typeInfo.createSerializer(env.getConfig)

    val original = new mutable.PriorityQueue[Int]()
    val rand = new Random(0)
    val values = List.tabulate(100)(_ => rand.nextInt(100))
    original.enqueue(values: _*)

    val copy = copyWithSerializer(original, serializer)

    assertEquals(original.length, copy.length)
    assertEquals(original.dequeueAll.toList, copy.dequeueAll.toList)
  }

  @Test
  def test_PriorityQueueTypeSerializer_Deserialize_AfterRestoring_WithQueueOfInt_With100RandomItems_ReturnsQueueThatYieldsSameItemsAsOriginal(): Unit = {
    val typeInfo = new PriorityQueueTypeInformation[Int](createTypeInformation[Int], Ordering.Int)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val serializer = typeInfo.createSerializer(env.getConfig)

    val snapshot = serializer.snapshotConfiguration()
    val snapshotCopy = new types.PriorityQueueTypeSerializer.Snapshot[Int]()
    copyData(snapshot.writeSnapshot, input => snapshotCopy.readSnapshot(snapshot.getCurrentVersion, input, getClass.getClassLoader))

    val serializerCopy = snapshotCopy.restoreSerializer()

    val original = new mutable.PriorityQueue[Int]()
    val rand = new Random(0)
    val values = List.tabulate(100)(_ => rand.nextInt(100))
    original.enqueue(values: _*)

    val copy = copyData(
      output => serializer.serialize(original, output),
      input => serializerCopy.deserialize(input))

    assertEquals(original.length, copy.length)
    assertEquals(original.dequeueAll.toList, copy.dequeueAll.toList)
  }

  @Test
  def test_PriorityQueueTypeSerializer_Deserialize_AfterRestoring_WithQueueOfRecordWrapperAndSequenceNumberOrdering_With100RandomItems_ReturnsQueueThatYieldsSameItemsAsOriginal(): Unit = {
    val ordering = new SequenceNumberOrdering[IntRecord, Product]
    val typeInfo = new PriorityQueueTypeInformation[RecordWrapper[IntRecord, Product]](
      RecordWrapperTypeInformation.wrap(createTypeInformation[IntRecord]),
      ordering)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val serializer = typeInfo.createSerializer(env.getConfig)

    val snapshot = serializer.snapshotConfiguration()
    val snapshotCopy = new types.PriorityQueueTypeSerializer.Snapshot[RecordWrapper[IntRecord, Product]]()
    copyData(snapshot.writeSnapshot, input => snapshotCopy.readSnapshot(snapshot.getCurrentVersion, input, getClass.getClassLoader))

    val serializerCopy = snapshotCopy.restoreSerializer()

    val original = new mutable.PriorityQueue[RecordWrapper[IntRecord, Product]]()(ordering)
    val rand = new Random(0)
    val values = List.tabulate(100)(i => RecordWrapper.wrap(IntRecord(rand.nextInt(100)), i.toLong))
    original.enqueue(values: _*)

    val copy = copyData(
      output => serializer.serialize(original, output),
      input => serializerCopy.deserialize(input))

    assertEquals(original.length, copy.length)
    assertEquals(original.dequeueAll.toList, copy.dequeueAll.toList)
  }
}
