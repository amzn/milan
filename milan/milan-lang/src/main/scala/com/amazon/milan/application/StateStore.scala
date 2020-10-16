package com.amazon.milan.application

import com.amazon.milan.serialization.{TypeInfoProvider, TypedJsonDeserializer, TypedJsonSerializer}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * Base trait for all state store classes.
 */
@JsonSerialize(using = classOf[StateStoreSerializer])
@JsonDeserialize(using = classOf[StateStoreDeserializer])
trait StateStore extends TypeInfoProvider


class StateStoreDeserializer extends TypedJsonDeserializer[StateStore]("com.amazon.milan.application.state")


class StateStoreSerializer extends TypedJsonSerializer[StateStore]

