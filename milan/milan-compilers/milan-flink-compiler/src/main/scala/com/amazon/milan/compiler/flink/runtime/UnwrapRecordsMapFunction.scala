package com.amazon.milan.compiler.flink.runtime

import com.amazon.milan.compiler.flink.types.RecordWrapper
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable


/**
 * A [[MapFunction]] that removes the wrapper from [[RecordWrapper]] objects.
 *
 * @param valueTypeInfo [[TypeInformation]] for the wrapped record type.
 * @tparam T The wrapped record type.
 */
class UnwrapRecordsMapFunction[T >: Null, TKey >: Null <: Product](valueTypeInfo: TypeInformation[T])
  extends MapFunction[RecordWrapper[T, TKey], T]
    with ResultTypeQueryable[T] {

  override def map(record: RecordWrapper[T, TKey]): T = {
    record.value
  }

  override def getProducedType: TypeInformation[T] = this.valueTypeInfo
}
