package com.amazon.milan.aws.serverless


trait ObjectStore[TKey, TValue] {
  def getItem(key: TKey): Option[TValue]

  def putItem(key: TKey, item: TValue)
}
