package com.amazon.milan.compiler.scala.event

object RecordWrapper {
  def wrap[T](value: T): RecordWrapper[T, Product] =
    new RecordWrapper[T, Product](value, None)
}


case class RecordWrapper[T, TKey](value: T, key: TKey) {
  def withValue[TNew](newValue: TNew): RecordWrapper[TNew, TKey] =
    RecordWrapper(newValue, this.key)

  def withKey[TNewKey](newKey: TNewKey): RecordWrapper[T, TNewKey] =
    RecordWrapper(this.value, newKey)
}
