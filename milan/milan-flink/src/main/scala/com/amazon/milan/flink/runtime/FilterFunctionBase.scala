package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.types.RecordWrapper
import org.apache.flink.api.common.functions.FilterFunction


abstract class FilterFunctionBase[T >: Null, TKey >: Null <: Product] extends FilterFunction[RecordWrapper[T, TKey]] {

  protected def filterImpl(value: T): Boolean

  override def filter(record: RecordWrapper[T, TKey]): Boolean = {
    this.filterImpl(record.value)
  }
}
