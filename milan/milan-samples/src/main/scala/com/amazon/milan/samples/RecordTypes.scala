package com.amazon.milan.samples

import java.time.Instant

import com.amazon.milan.Id
import com.amazon.milan.types.Record


class DateValueRecord(var recordId: String,
                      var dateTime: Instant,
                      var value: Double) extends Record {
  def this(dateTime: Instant, value: Double) {
    this(Id.newId(), dateTime, value)
  }

  override def getRecordId: String = this.recordId

  override def toString: String = s"(${this.dateTime}, ${this.value}))"
}

object DateValueRecord {
  def apply(dateTime: Instant, value: Double): DateValueRecord = new DateValueRecord(dateTime, value)
}


class KeyValueRecord(var recordId: String,
                     var key: Int,
                     var value: Int) extends Record {
  def this(key: Int, value: Int) {
    this(Id.newId(), key, value)
  }

  override def getRecordId: String = this.recordId

  override def toString: String = s"(${this.key}, ${this.value}))"
}

object KeyValueRecord {
  def apply(key: Int, value: Int): KeyValueRecord = new KeyValueRecord(key, value)
}
