package com.amazon.milan.types

import com.amazon.milan.Id


class LongRecord(var recordId: String, var value: Long) extends Serializable {
  def this(value: Long) {
    this(Id.newId(), value)
  }

  def this() {
    this(0)
  }
}
