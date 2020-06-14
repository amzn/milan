package com.amazon.milan.compiler.flink.runtime


trait RecordIdExtractor[T] extends Serializable {
  def canExtractRecordId: Boolean

  def apply(record: T): String
}
