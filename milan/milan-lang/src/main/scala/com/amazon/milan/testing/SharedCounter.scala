package com.amazon.milan.testing

import java.util.UUID

import scala.collection.mutable


object SharedCounter {
  private val values = new mutable.HashMap[String, Int]()

  def create(): SharedCounter = {
    val id = UUID.randomUUID().toString
    SharedCounter.values.put(id, 0)
    new SharedCounter(id)
  }
}


class SharedCounter(id: String) extends Serializable {
  def getValue: Int = SharedCounter.values(this.id)

  def +=(value: Int): Int = {
    val currentValue = SharedCounter.values(this.id)
    SharedCounter.values.put(this.id, currentValue + value)
    currentValue
  }
}
