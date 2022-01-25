package com.amazon.milan

import scala.collection.immutable.HashSet
import scala.language.implicitConversions


package object serialization {

  implicit class SetExtensions[T](set: Set[T]) {
    def toHashSet: HashSet[T] = {
      val hashSet = new HashSet[T]()
      hashSet.union(this.set)
    }
  }

  implicit class MapExtensions[TKey, TValue](map: Map[TKey, TValue]) {
    def toJavaMap: java.util.Map[TKey, TValue] = {
      val hashMap = new java.util.HashMap[TKey, TValue]()
      this.map.foreach {
        case (key, value) => hashMap.put(key, value)
      }
      hashMap
    }
  }

}
