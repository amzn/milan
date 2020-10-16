package com.amazon.milan.application.state

import com.amazon.milan.application.StateStore
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * A [[StateStore]] that specifies that the default state storage for the selected runtime should be used.
 */
@JsonSerialize
@JsonDeserialize
class DefaultStateStore extends StateStore

