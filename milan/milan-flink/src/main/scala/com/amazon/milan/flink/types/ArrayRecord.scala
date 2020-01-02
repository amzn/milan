package com.amazon.milan.flink.types

import com.amazon.milan.Id


object ArrayRecord {
  val typeName: String = getClass.getTypeName.stripSuffix("$")

  def empty(length: Int): ArrayRecord = ArrayRecord("", Array.fill[Any](length)(null))

  def apply(values: Array[Any]): ArrayRecord = ArrayRecord(Id.newId(), values)

  def apply(recordId: String, values: Array[Any]): ArrayRecord = new ArrayRecord(recordId, values)

  def fromElements(elements: Any*): ArrayRecord = ArrayRecord(Id.newId(), elements.toArray)
}


/**
 * Record type for tuple streams when Milan applications are compiled to Flink.
 * This must be a POJO type.
 */
class ArrayRecord(var recordId: String, var values: Array[Any]) {
  def this() {
    this("", Array())
  }

  def apply(index: Int): Any = this.values(index)

  override def clone(): ArrayRecord = ArrayRecord(this.recordId, values.clone())

  def sameElements(other: ArrayRecord): Boolean = this.values.sameElements(other.values)

  def sameElements(other: Array[Any]): Boolean = this.values.sameElements(other)
}
