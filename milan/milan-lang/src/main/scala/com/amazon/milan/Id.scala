package com.amazon.milan

import java.util.UUID


object Id {
  def newId(): String = UUID.randomUUID().toString
}
