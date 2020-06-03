package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.types.{RecordWrapper, RecordWrapperTypeInformation}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.util.Collector


/**
 * A Flink [[FlatMapFunction]] that passes input records through as-is.
 */
class IdentityFlatMapFunction[T >: Null, TKey >: Null <: Product](recordTypeInformation: TypeInformation[T],
                                                                  keyTypeInformation: TypeInformation[TKey])
  extends FlatMapFunction[RecordWrapper[T, TKey], RecordWrapper[T, TKey]]
    with ResultTypeQueryable[RecordWrapper[T, TKey]] {

  override def flatMap(record: RecordWrapper[T, TKey], collector: Collector[RecordWrapper[T, TKey]]): Unit = {
    collector.collect(record)
  }

  override def getProducedType: TypeInformation[RecordWrapper[T, TKey]] =
    RecordWrapperTypeInformation.wrap(this.recordTypeInformation, this.keyTypeInformation)
}
