package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.types.RecordWrapper
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector


/**
 * A Flink [[KeySelector]] that selects the key from [[RecordWrapper]] objects.
 */
class RecordWrapperKeySelector[T >: Null, TKey >: Null <: Product](keyTypeInfo: TypeInformation[TKey])
  extends KeySelector[RecordWrapper[T, TKey], TKey] {

  def getKeyType: TypeInformation[TKey] = this.keyTypeInfo

  override def getKey(record: RecordWrapper[T, TKey]): TKey = record.key
}
