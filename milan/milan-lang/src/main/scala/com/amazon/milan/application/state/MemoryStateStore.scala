package com.amazon.milan.application.state

import com.amazon.milan.application.StateStore
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}


/**
 * A [[StateStore]] that stores state in local memory.
 */
@JsonSerialize
@JsonDeserialize
class MemoryStateStore extends StateStore
