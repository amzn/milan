package com.amazon.milan.flink.components

import com.amazon.milan.types.{LineageRecord, Record, RecordWithLineage}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala.createTypeInformation


object RecordWithLineageTypeInformation {
  /**
   * A Flink [[TypeInformation]] instance for the [[LineageRecord]] class.
   */
  val lineageRecordTypeInformation: TypeInformation[LineageRecord] = createTypeInformation[LineageRecord]
}


/**
 * A Flink [[TypeInformation]] class for [[RecordWithLineage]] types.
 *
 * @param recordTypeInformation The [[TypeInformation]] for the record portion of the [[RecordWithLineage]] type.
 * @tparam T The record type parameter of the [[RecordWithLineage]] type.
 */
class RecordWithLineageTypeInformation[T <: Record](val recordTypeInformation: TypeInformation[T])
  extends TypeInformation[RecordWithLineage[T]] {

  override def isBasicType: Boolean = false

  override def isTupleType: Boolean = false

  override def getArity: Int = 2

  override def getTotalFields: Int =
    this.recordTypeInformation.getTotalFields + RecordWithLineageTypeInformation.lineageRecordTypeInformation.getTotalFields + 2

  override def getTypeClass: Class[RecordWithLineage[T]] =
    classOf[RecordWithLineage[T]]

  override def isKeyType: Boolean = false

  override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[RecordWithLineage[T]] = {
    new RecordWithLineageSerializer[T](
      this.recordTypeInformation.createSerializer(executionConfig),
      RecordWithLineageTypeInformation.lineageRecordTypeInformation.createSerializer(executionConfig))
  }

  override def canEqual(o: Any): Boolean = o.isInstanceOf[RecordWithLineageTypeInformation[T]]

  override def toString: String = s"RecordWithLineage[${this.recordTypeInformation}]"

  override def hashCode(): Int = this.recordTypeInformation.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case o: RecordWithLineageTypeInformation[T] =>
      this.recordTypeInformation.equals(o.recordTypeInformation)

    case _ =>
      false
  }
}
