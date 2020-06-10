package com.amazon.milan

import java.util.UUID


object Id {
  /**
   * Generates a UUID string.
   */
  def newId(): String = UUID.randomUUID().toString
}
