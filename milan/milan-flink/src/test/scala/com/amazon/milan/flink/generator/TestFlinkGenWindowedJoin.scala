package com.amazon.milan.flink.generator

import com.amazon.milan.flink.testing.{DateKeyValueRecord, IntKeyValueRecord, IntRecord}
import com.amazon.milan.lang._
import org.junit.Test


object TestFlinkGenWindowedJoin {
  def processWindow(leftRecord: IntRecord, window: Iterable[DateKeyValueRecord]): IntKeyValueRecord = {
    window.find(_.key == leftRecord.i) match {
      case Some(rightRecord) =>
        IntKeyValueRecord(leftRecord.i, rightRecord.value)

      case None =>
        // TODO: Find something better to do.
        throw new Exception("Window is empty.")
    }
  }
}


@Test
class TestFlinkGenWindowedJoin {
  @Test
  def test_FlinkGenWindowedJoin(): Unit = {
    // TODO: Finish this.
    val leftInput = Stream.of[IntRecord]
    val rightInput = Stream.of[DateKeyValueRecord]

    def maxByDateTime(stream: Stream[DateKeyValueRecord]): Stream[DateKeyValueRecord] =
      stream.maxBy(r => r.dateTime)

    val rightWindow = rightInput.groupBy(r => r.key).map((_, group) => maxByDateTime(group))
  }

}
