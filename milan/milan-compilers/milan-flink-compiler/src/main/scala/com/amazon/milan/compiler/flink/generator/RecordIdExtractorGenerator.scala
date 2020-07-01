package com.amazon.milan.compiler.flink.generator

import com.amazon.milan.compiler.scala._
import com.amazon.milan.compiler.flink.runtime.RecordIdExtractor
import com.amazon.milan.compiler.flink.types.ArrayRecord
import com.amazon.milan.compiler.flink.typeutil._
import com.amazon.milan.typeutil.TypeDescriptor


trait RecordIdExtractorGenerator {
  val typeLifter: FlinkTypeLifter

  import typeLifter._

  /**
   * Generates a code snippet that extracts the record ID from a record val.
   *
   * @param recordValName The name of the val whose record ID is being extracted.
   * @param recordType    The type of the records.
   * @return A code snippet that extracts the record ID from the val.
   */
  def generateRecordIdStatement(recordValName: ValName,
                                recordType: TypeDescriptor[_]): CodeBlock = {
    if (recordType.isNamedTuple) {
      qc"$recordValName.recordId"
    }
    else {
      val recordIdFields = Set("recordId", "getRecordId")

      recordType.fields.find(field => recordIdFields.contains(field.name)) match {
        case Some(recordIdField) =>
          qc"$recordValName.${code(recordIdField.name)}"

        case None =>
          qc"$recordValName.toString"
      }
    }
  }

  /**
   * Generates a [[RecordIdExtractor]] for a type.
   *
   * @param output         The generator output collector.
   * @param typeDescriptor A [[TypeDescriptor]] for the type for which an extractor is needed.
   * @return The name of the class that implements the [[com.amazon.milan.compiler.flink.runtime.RecordIdExtractor]].
   */
  def generateRecordIdExtractor(output: GeneratorOutputs,
                                typeDescriptor: TypeDescriptor[_]): ClassName = {
    if (output.recordIdExtractorClasses.contains(typeDescriptor.fullName)) {
      return output.recordIdExtractorClasses(typeDescriptor.fullName)
    }
    else if (typeDescriptor.isTupleRecord) {
      return nameOf[ArrayRecordIdExtractor]
    }

    val recordIdFields = Set("recordId", "getRecordId")

    typeDescriptor.fields.find(field => recordIdFields.contains(field.name)) match {
      case None =>
        ClassName(q"${nameOf[NoRecordIdExtractor[Any]]}[${typeDescriptor.toTerm}]")

      case Some(recordIdField) =>
        val className = output.newClassName("RecordIdExtractor_")

        val classDef =
          q"""
             |class $className extends ${nameOf[RecordIdExtractor[Any]]}[${typeDescriptor.toTerm}] {
             |  override def canExtractRecordId: Boolean = true
             |
             |  override def apply(record: ${typeDescriptor.toTerm}): String =
             |    record.${code(recordIdField.name)}.toString()
             |}
             |""".codeStrip

        output.addClassDef(classDef)

        className
    }
  }
}


class NoRecordIdExtractor[T] extends RecordIdExtractor[T] {
  override def canExtractRecordId: Boolean = false

  override def apply(record: T): String = throw new NotImplementedError()
}


class ArrayRecordIdExtractor extends RecordIdExtractor[ArrayRecord] {
  override def canExtractRecordId: Boolean = true

  override def apply(record: ArrayRecord): String = record.recordId
}