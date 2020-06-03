package com.amazon.milan.flink.testutil

import com.amazon.milan.SemanticVersion
import com.amazon.milan.flink.internal.{JoinLineageRecordFactory, LineageRecordFactory}
import com.amazon.milan.types.{LineageRecord, RecordPointer}


/**
 * A [[LineageRecordFactory]] that outputs records with garbage information.
 */
object EmptyLineageRecordFactory extends LineageRecordFactory {
  override def createLineageRecord(targetRecordId: String, sourceRecords: Seq[RecordPointer]): LineageRecord =
    LineageRecord("", "", targetRecordId, "", "", SemanticVersion.ZERO, sourceRecords.toArray)

  override def createRecordPointer(sourceRecordId: String): RecordPointer = RecordPointer("", sourceRecordId)
}


/**
 * A [[JoinLineageRecordFactory]] that outputs records with garbage information.
 */
object EmptyJoinLineageRecordFactory extends JoinLineageRecordFactory {
  override def createLineageRecord(targetRecordId: String, sourceRecords: Seq[RecordPointer]): LineageRecord =
    LineageRecord("", "", targetRecordId, "", "", SemanticVersion.ZERO, sourceRecords.toArray)

  override def createLeftRecordPointer(sourceRecordId: String): RecordPointer = RecordPointer("", sourceRecordId)

  override def createRightRecordPointer(sourceRecordId: String): RecordPointer = RecordPointer("", sourceRecordId)
}
