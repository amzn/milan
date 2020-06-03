package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.types.{RecordWrapper, RecordWrapperTypeInformation}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable


/**
 * A Flink [[MapFunction]] that modifies the key of records.
 */
abstract class ModifyRecordKeyMapFunction[T >: Null, TInKey >: Null <: Product, TOutKey >: Null <: Product](valueTypeInfo: TypeInformation[T],
                                                                                                            outKeyTypeInfo: TypeInformation[TOutKey])
  extends MapFunction[RecordWrapper[T, TInKey], RecordWrapper[T, TOutKey]]
    with ResultTypeQueryable[RecordWrapper[T, TOutKey]] {

  protected def getNewKey(value: T, key: TInKey): TOutKey

  override def map(record: RecordWrapper[T, TInKey]): RecordWrapper[T, TOutKey] = {
    val newKey = this.getNewKey(record.value, record.key)
    record.withKey(newKey)
  }

  override def getProducedType: TypeInformation[RecordWrapper[T, TOutKey]] =
    RecordWrapperTypeInformation.wrap(this.valueTypeInfo, this.outKeyTypeInfo)
}
