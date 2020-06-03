package com.amazon.milan.flink.internal

import com.amazon.milan.types.{LineageRecord, RecordPointer}
import com.amazon.milan.{Id, SemanticVersion}


trait LineageRecordFactory extends Serializable {
  def createLineageRecord(targetRecordId: String,
                          sourceRecords: Seq[RecordPointer]): LineageRecord

  def createRecordPointer(sourceRecordId: String): RecordPointer
}


trait JoinLineageRecordFactory extends Serializable {
  def createLineageRecord(targetRecordId: String,
                          sourceRecords: Seq[RecordPointer]): LineageRecord

  def createLeftRecordPointer(sourceRecordId: String): RecordPointer

  def createRightRecordPointer(sourceRecordId: String): RecordPointer
}


class ComponentLineageRecordFactory(inputStreamId: String,
                                    applicationInstanceId: String,
                                    outputStreamId: String,
                                    componentId: String,
                                    componentVersion: SemanticVersion)
  extends LineageRecordFactory {

  override def createLineageRecord(targetRecordId: String, sourceRecords: Seq[RecordPointer]): LineageRecord = {
    LineageRecord(
      Id.newId(),
      this.outputStreamId,
      targetRecordId,
      this.applicationInstanceId,
      this.componentId,
      this.componentVersion,
      sourceRecords.toArray)
  }

  override def createRecordPointer(sourceRecordId: String): RecordPointer = {
    RecordPointer(this.inputStreamId, sourceRecordId)
  }
}


class ComponentJoinLineageRecordFactory(leftInputStreamId: String,
                                        rightInputStreamId: String,
                                        applicationInstanceId: String,
                                        outputStreamId: String,
                                        componentId: String,
                                        componentVersion: SemanticVersion)
  extends JoinLineageRecordFactory {

  override def createLineageRecord(targetRecordId: String, sourceRecords: Seq[RecordPointer]): LineageRecord = {
    LineageRecord(
      Id.newId(),
      this.outputStreamId,
      targetRecordId,
      this.applicationInstanceId,
      this.componentId,
      this.componentVersion,
      sourceRecords.toArray)
  }

  override def createLeftRecordPointer(sourceRecordId: String): RecordPointer = {
    RecordPointer(this.leftInputStreamId, sourceRecordId)
  }

  override def createRightRecordPointer(sourceRecordId: String): RecordPointer = {
    RecordPointer(this.rightInputStreamId, sourceRecordId)
  }
}
