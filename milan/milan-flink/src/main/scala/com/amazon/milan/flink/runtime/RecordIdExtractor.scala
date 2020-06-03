package com.amazon.milan.flink.runtime


trait RecordIdExtractor[T] extends Serializable {
  def canExtractRecordId: Boolean

  def apply(record: T): String
}
