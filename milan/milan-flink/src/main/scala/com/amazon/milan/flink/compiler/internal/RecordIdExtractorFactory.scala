package com.amazon.milan.flink.compiler.internal

import com.amazon.milan.flink.RuntimeEvaluator
import com.amazon.milan.flink.types.ArrayRecord
import com.amazon.milan.typeutil.TypeDescriptor


object RecordIdExtractorFactory {
  /**
   * Gets a function that extracts record IDs from record objects.
   *
   * @param recordType A [[TypeDescriptor]] describing the record objects.
   * @tparam T The type of the record objects.
   * @return A function that extracts record IDs, or None if no record ID can be found on the type.
   */
  def getRecordIdExtractor[T](recordType: TypeDescriptor[T]): Option[T => String] = {
    if (recordType.isTuple) {
      // Tuple types use ArrayRecord as their record type, which has a recordId property.
      Some((record: T) => record.asInstanceOf[ArrayRecord].recordId)
    }
    else {
      val recordIdFields = Set("recordId", "getRecordId")

      recordType.fields.find(field => recordIdFields.contains(field.name)) match {
        case Some(recordField) =>
          Some(
            RuntimeEvaluator.instance.createFunction[T, String](
              "record",
              recordType.fullName,
              s"record.${recordField.name}.toString()"))

        case None => None
      }
    }
  }
}
