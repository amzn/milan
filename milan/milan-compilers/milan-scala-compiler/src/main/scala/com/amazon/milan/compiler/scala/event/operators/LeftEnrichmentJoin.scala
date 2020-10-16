package com.amazon.milan.compiler.scala.event.operators

import com.amazon.milan.compiler.scala.event.{KeyedStateInterface, RecordWrapper}


abstract class LeftEnrichmentJoin[TLeftValue, TLeftKey, TRightValue >: Null, TRightKey, TJoinKey] {
  /**
   * State interface used to store the most recent record for each join key from the right stream.
   */
  protected val rightState: KeyedStateInterface[TJoinKey, TRightValue]

  protected def getLeftJoinKey(leftValue: TLeftValue): TJoinKey

  protected def getRightJoinKey(rightValue: TRightValue): TJoinKey

  protected def checkLeftPreCondition(leftValue: TLeftValue): Boolean

  protected def checkRightPreCondition(rightValue: TRightValue): Boolean

  protected def checkPostCondition(leftValue: TLeftValue, rightValue: TRightValue): Boolean

  def processLeftRecord(leftRecord: RecordWrapper[TLeftValue, TLeftKey]): Option[RecordWrapper[(TLeftValue, TRightValue), Product]] = {
    if (!this.checkLeftPreCondition(leftRecord.value)) {
      None
    }
    else {
      val joinKey = this.getLeftJoinKey(leftRecord.value)

      val rightValue = this.rightState.getState(joinKey).orNull

      if (this.checkPostCondition(leftRecord.value, rightValue)) {
        Some(RecordWrapper.wrap((leftRecord.value, rightValue)))
      }
      else {
        None
      }
    }
  }

  def processRightRecord(rightRecord: RecordWrapper[TRightValue, TRightKey]): Option[RecordWrapper[(TLeftValue, TRightValue), Product]] = {
    if (this.checkRightPreCondition(rightRecord.value)) {
      val joinKey = this.getRightJoinKey(rightRecord.value)
      this.rightState.setState(joinKey, rightRecord.value)
    }

    None
  }
}
