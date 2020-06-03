package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.types.{ArrayRecord, RecordWrapper, RecordWrapperTypeInformation}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable


abstract class ArrayRecordToTupleMapFunction[T >: Null, TKey >: Null <: Product](outputTypeInformation: TypeInformation[T],
                                                                                 keyTypeInformation: TypeInformation[TKey])
  extends MapFunction[RecordWrapper[ArrayRecord, TKey], RecordWrapper[T, TKey]]
    with ResultTypeQueryable[RecordWrapper[T, TKey]] {

  protected def getTuple(record: ArrayRecord): T

  override def map(record: RecordWrapper[ArrayRecord, TKey]): RecordWrapper[T, TKey] = {
    val tupleValue = this.getTuple(record.value)
    RecordWrapper.wrap[T, TKey](tupleValue, record.key, 0)
  }

  override def getProducedType: TypeInformation[RecordWrapper[T, TKey]] =
    RecordWrapperTypeInformation.wrap(this.outputTypeInformation, this.keyTypeInformation)
}
