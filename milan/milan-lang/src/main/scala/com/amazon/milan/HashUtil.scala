package com.amazon.milan


object HashUtil {
  /**
   * Derives a hash code from other hash codes.
   *
   * @param codes A sequence of hash codes.
   * @return A new hash code derived from the input codes.
   */
  def combineHashCodes(codes: Iterable[Int]): Int = {
    var hash1 = (5381 << 16) + 5381
    var hash2 = hash1

    var i = 0
    codes.foreach(code => {
      if (i % 2 == 0) {
        hash1 = ((hash1 << 5) + hash1 + (hash1 >> 27)) ^ code
      }
      else {
        hash2 = ((hash2 << 5) + hash2 + (hash2 >> 27)) ^ code
      }

      i += 1
    })

    hash1 + (hash2 * 1566083941)
  }

  def combineHashCodes(codes: Int*): Int = {
    combineHashCodes(codes)
  }

  def combineObjectHashCodes(objects: Seq[Any]): Int = {
    combineHashCodes(objects.map(o => o.hashCode()))
  }

  def hashCodeOfMapItems[A, B](map: Map[A, B]): TraversableOnce[Int] = {
    map.keys.map(key => key.hashCode()) ++ map.values.map(value => value.hashCode())
  }
}
