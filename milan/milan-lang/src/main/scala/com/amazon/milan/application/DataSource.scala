package com.amazon.milan.application

import com.amazon.milan.serialization.{GenericTypeInfoProvider, GenericTypedJsonDeserializer, GenericTypedJsonSerializer, SetGenericTypeInfo}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * Base trait for data source classes.
 *
 * @tparam T The type of objects produced by the data source.
 */
@JsonSerialize(using = classOf[DataSourceSerializer])
@JsonDeserialize(using = classOf[DataSourceDeserializer])
trait DataSource[T] extends GenericTypeInfoProvider with SetGenericTypeInfo


class DataSourceSerializer extends GenericTypedJsonSerializer[DataSource[_]]


class DataSourceDeserializer extends GenericTypedJsonDeserializer[DataSource[_]]("com.amazon.milan.application.sources")
