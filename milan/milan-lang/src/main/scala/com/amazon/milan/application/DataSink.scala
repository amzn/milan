package com.amazon.milan.application

import com.amazon.milan.serialization.{GenericTypeInfoProvider, GenericTypedJsonDeserializer, GenericTypedJsonSerializer, SetGenericTypeInfo}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * Base trait for data sink classes.
 *
 * @tparam T The type of objects accepted by the data sink.
 */
@JsonSerialize(using = classOf[DataSinkSerializer])
@JsonDeserialize(using = classOf[DataSinkDeserializer])
trait DataSink[T] extends GenericTypeInfoProvider with SetGenericTypeInfo


class DataSinkDeserializer extends GenericTypedJsonDeserializer[DataSink[_]]("com.amazon.milan.application.sinks")


class DataSinkSerializer extends GenericTypedJsonSerializer[DataSink[_]]
