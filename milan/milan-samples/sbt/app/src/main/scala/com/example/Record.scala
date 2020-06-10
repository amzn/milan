package com.example

import com.amazon.milan.Id


/**
 * A basic record class for our example.
 */
class Record(var recordId: String, var value: Int) extends Serializable {
  def this() {
    this("", 0)
  }
}


object Record {
  def apply(value: Int): Record = new Record(Id.newId(), value)
}
