package com.amazon.milan.application

import com.amazon.milan.serialization.{GenericTypeInfoProvider, GenericTypedJsonDeserializer, GenericTypedJsonSerializer, SetGenericTypeInfo}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * Base trait for metric definition classes.
 */
@JsonSerialize(using = classOf[MetricDefinitionSerializer])
@JsonDeserialize(using = classOf[MetricDefinitionDeserializer])
trait MetricDefinition[+T] extends GenericTypeInfoProvider with SetGenericTypeInfo with Serializable


class MetricDefinitionSerializer extends GenericTypedJsonSerializer[MetricDefinition[_]]


class MetricDefinitionDeserializer extends GenericTypedJsonDeserializer[MetricDefinition[_]]("com.amazon.milan.application.metrics")
