package com.amazon.milan.flink

import com.amazon.milan.flink.types.ArrayRecord
import com.amazon.milan.typeutil.TypeDescriptor


package object typeutil {

  implicit class FlinkTypeUtilTypeDescriptorExtensions[_](typeDesc: TypeDescriptor[_]) {
    def getTypeName: String =
      this.typeDesc.fullName

    def getRecordTypeName: String = {
      if (this.typeDesc.isStream) {
        this.typeDesc.asStream.recordType.getRecordTypeName
      }
      else if (this.typeDesc.isTupleRecord) {
        ArrayRecord.typeName
      }
      else {
        this.getTypeName
      }
    }

    def getRecordType: TypeDescriptor[_] = {
      if (this.typeDesc.isStream) {
        this.typeDesc.asStream.recordType
      }
      else {
        this.typeDesc
      }
    }

    def isTupleRecord: Boolean = this.typeDesc.isInstanceOf[TupleRecordTypeDescriptor[_]]

    def toTupleRecord: TupleRecordTypeDescriptor[_] =
      new TupleRecordTypeDescriptor[Any](this.typeDesc.fields)
  }

}
