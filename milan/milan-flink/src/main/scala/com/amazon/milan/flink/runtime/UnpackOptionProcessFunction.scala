package com.amazon.milan.flink.runtime

import com.amazon.milan.flink.types.{RecordWrapper, RecordWrapperTypeInformation}
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


class UnpackOptionProcessFunction[T >: Null, TKey >: Null <: Product](recordType: TypeInformation[T],
                                                                      keyType: TypeInformation[TKey])
  extends ProcessFunction[RecordWrapper[Option[T], TKey], RecordWrapper[T, TKey]]
    with ResultTypeQueryable[RecordWrapper[T, TKey]] {

  @transient private lazy val logger = Logger(LoggerFactory.getLogger(getClass))

  override def processElement(record: RecordWrapper[Option[T], TKey],
                              context: ProcessFunction[RecordWrapper[Option[T], TKey], RecordWrapper[T, TKey]]#Context,
                              collector: Collector[RecordWrapper[T, TKey]]): Unit = {
    if (record.value.isDefined) {
      collector.collect(RecordWrapper.wrap(record.value.get, record.key, record.sequenceNumber))
    }
  }

  override def getProducedType: TypeInformation[RecordWrapper[T, TKey]] =
    RecordWrapperTypeInformation.wrap(this.recordType, this.keyType)
}
