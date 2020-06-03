package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.types.{RecordWrapper, RecordWrapperTypeInformation}
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable


/**
 * A [[MapFunction]] that wraps records in [[RecordWrapper]] objects.
 *
 * @param recordTypeInformation [[TypeInformation]] for the input record type.
 * @tparam T The input record type.
 */
class WrapRecordsMapFunction[T >: Null](recordTypeInformation: TypeInformation[T])
  extends RichMapFunction[T, RecordWrapper[T, Product]]
    with ResultTypeQueryable[RecordWrapper[T, Product]] {

  override def map(value: T): RecordWrapper[T, Product] =
    RecordWrapper.wrap[T](value, 0)

  override def getProducedType: TypeInformation[RecordWrapper[T, Product]] =
    RecordWrapperTypeInformation.wrap(this.recordTypeInformation)
}
