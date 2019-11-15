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

}
