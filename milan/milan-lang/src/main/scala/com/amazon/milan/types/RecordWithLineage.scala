package com.amazon.milan.types


object RecordWithLineage {
  val typeName: String = getClass.getTypeName.stripSuffix("$")
}

case class RecordWithLineage[T <: Record](record: T, lineage: LineageRecord)
